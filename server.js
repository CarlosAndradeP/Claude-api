const express = require('express');
const { Readable } = require('stream');
const fs = require('fs');
const path = require('path');

const adminApp = express();
const proxyApp = express();

const DATA_DIR = path.join(__dirname, 'data');
const KEYS_FILE = path.join(DATA_DIR, 'keys.json');

// Garante que a pasta data existe (para o volume Docker)
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
}

let keys = [];

// Carrega as chaves do disco
if (fs.existsSync(KEYS_FILE)) {
    try {
        keys = JSON.parse(fs.readFileSync(KEYS_FILE, 'utf8'));
    } catch (e) {
        console.error("Erro ao carregar chaves:", e);
    }
}

function saveKeys() {
    fs.writeFileSync(KEYS_FILE, JSON.stringify(keys, null, 2));
}

// ==========================================
// 1. SERVIDOR ADMIN (Porta 88)
// ==========================================
adminApp.use(express.json());
adminApp.use(express.static(path.join(__dirname, 'public')));

let proxyConfig = { disableAbort: false, transparentMode: true };

adminApp.get('/api/config', (req, res) => res.json(proxyConfig));
adminApp.post('/api/config', (req, res) => {
    if (req.body.disableAbort !== undefined) proxyConfig.disableAbort = !!req.body.disableAbort;
    if (req.body.transparentMode !== undefined) proxyConfig.transparentMode = !!req.body.transparentMode;
    res.json(proxyConfig);
});

adminApp.get('/api/keys', (req, res) => res.json(keys));

adminApp.post('/api/keys', (req, res) => {
    const { key, role, name } = req.body;
    const newKey = { id: Date.now().toString(), key, role, name };
    keys.push(newKey);
    saveKeys();
    res.json(newKey);
});

adminApp.delete('/api/keys/:id', (req, res) => {
    keys = keys.filter(k => k.id !== req.params.id);
    saveKeys();
    res.json({ success: true });
});

adminApp.listen(88, () => {
    console.log('✅ Painel Administrativo rodando na porta 88');
});


// ==========================================
// 2. SERVIDOR PROXY (Porta 11431)
// ==========================================
proxyApp.use((req, res, next) => {
    // Configura o CORS básico pro proxy caso seja chamado por browser
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', '*');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});
proxyApp.use(express.raw({ type: '*/*', limit: '50mb' })); // Permite ler qualquer body (json, multipart, etc) como Buffer

let subKeyIndex = 0;
let allKeyIndex = 0;

const COOLDOWN_MS = 65 * 1000;
const keyCooldowns = new Map(); // keyId -> timestamp (ms) de liberação após HTTP 429

function isKeyInCooldown(keyObj) {
    const expiry = keyCooldowns.get(keyObj.id);
    if (!expiry) return false;
    if (Date.now() < expiry) return true;
    keyCooldowns.delete(keyObj.id); // libera e evita crescimento indefinido do Map
    return false;
}

proxyApp.all('/v1/*', async (req, res) => {
    const agentType = (req.headers['x-agent-type'] || 'none').toLowerCase();
    
    console.log(`\n\n======================================================`);
    console.log(`[Proxy 📥] NOVA REQUISIÇÃO RECEBIDA`);
    console.log(`[Proxy ℹ️] Método: ${req.method} | Rota: ${req.url} | AgentType: ${agentType}`);
    
    // Mostra os headers omitindo autorização se houver
    const safeHeaders = { ...req.headers };
    delete safeHeaders['authorization'];
    console.log(`[Proxy ℹ️] Headers:`, JSON.stringify(safeHeaders));

    if (Buffer.isBuffer(req.body)) {
        console.log(`[Proxy ℹ️] Tamanho do Body: ${req.body.length} bytes`);
        if (req.body.length > 0) {
            try {
                console.log(`[Proxy ℹ️] Body Preview:`, req.body.toString('utf8').substring(0, 800));
            } catch(e) {}
        }
    }

    // Categoriza chaves
    const mainKeys = keys.filter(k => k.role === 'main' || k.role === 'all');
    const subKeys = keys.filter(k => k.role === 'sub' || k.role === 'all');
    const allKeys = keys;

    if (allKeys.length === 0) {
        console.error(`[Proxy 🛑] Erro: Nenhuma chave cadastrada!`);
        return res.status(500).json({ error: "Nenhuma chave configurada no proxy. Acesse a porta 88 para adicionar." });
    }

    // Define a ordem de tentativa (Pipeline)
    let pipeline = [];
    if (agentType === 'main' && mainKeys.length > 0) {
        pipeline = [...mainKeys, ...subKeys]; 
    } else if (agentType === 'sub' && subKeys.length > 0) {
        const startIdx = subKeyIndex % subKeys.length;
        subKeyIndex++;
        pipeline = [...subKeys.slice(startIdx), ...subKeys.slice(0, startIdx), ...mainKeys];
    } else {
        const startIdx = allKeyIndex % allKeys.length;
        allKeyIndex++;
        pipeline = [...allKeys.slice(startIdx), ...allKeys.slice(0, startIdx)];
    }

    pipeline = [...new Map(pipeline.map(item => [item.key, item])).values()];

    const antesDoCooldown = pipeline.length;
    pipeline = pipeline.filter(k => !isKeyInCooldown(k));
    const puladas = antesDoCooldown - pipeline.length;
    if (puladas > 0) {
        console.log(`[Proxy ⏳] ${puladas} chave(s) em cooldown (429) puladas nesta requisição.`);
    }

    if (pipeline.length === 0) {
        console.error(`[Proxy 🛑] Todas as chaves estão em cooldown (rate limit 429).`);
        return res.status(429).json({ error: "Todas as chaves disponíveis estão em cooldown após rate limit (429). Aguarde alguns segundos e tente novamente." });
    }

    const targetUrl = `https://integrate.api.nvidia.com${req.url}`;

    for (const keyObj of pipeline) {
        const controller = new AbortController();
        let clientClosed = false;
        
        const onClientDisconnect = () => {
            console.warn(`[Proxy ⚠️] EVENTO: Cliente encerrou a conexão HTTP abruptamente (Aborted)!`);
            clientClosed = true;
            if (!proxyConfig.disableAbort) {
                console.warn(`[Proxy ⚠️] Abortando requisição para NVIDIA...`);
                controller.abort();
            } else {
                console.warn(`[Proxy ⚠️] Abort desativado nas configurações. A requisição vai continuar.`);
            }
        };
        req.on('aborted', onClientDisconnect);

        try {
            const fetchOptions = {
                method: req.method,
                headers: {
                    'Authorization': `Bearer ${keyObj.key}`,
                    'Content-Type': req.headers['content-type'] || 'application/json',
                    'Accept': req.headers['accept'] || '*/*',
                    'Accept-Encoding': 'identity'
                }
            };

            if (req.method !== 'GET' && req.method !== 'HEAD' && req.body) {
                if (proxyConfig.transparentMode) {
                    // TRANSPARENT MODE: No JSON parsing, no modifications, no Fast-Track.
                    // Just raw pass-through of the exact bytes received.
                    fetchOptions.body = req.body;
                    if (Buffer.isBuffer(req.body)) {
                        fetchOptions.headers['Content-Length'] = req.body.length.toString();
                    }
                    console.log(`[Proxy 🔍] Modo Transparente Ativo: Payload repassado sem modificações.`);
                } else {
                    let finalBodyStr = "";
                    let finalBodyBuffer = null;
                    
                    // Intercepta e corrige os parâmetros para o modelo glm-5.1
                    if (Buffer.isBuffer(req.body) && req.body.length > 0) {
                        try {
                            const jsonBody = JSON.parse(req.body.toString('utf8'));
                            
                            // Força os limites e parâmetros exigidos pela documentação da NVIDIA NIM
                            if (jsonBody.model && jsonBody.model.includes('glm-5.1')) {
                                if (jsonBody.max_tokens > 16384) {
                                    jsonBody.max_tokens = 16384;
                                }
                                // Adiciona os parâmetros recomendados se não existirem
                                if (jsonBody.temperature === undefined) jsonBody.temperature = 1;
                                if (jsonBody.top_p === undefined) jsonBody.top_p = 1;
                            }
                            finalBodyStr = JSON.stringify(jsonBody);
                        } catch (e) {
                            finalBodyStr = req.body.toString('utf8');
                        }
                    } else if (!Buffer.isBuffer(req.body) && Object.keys(req.body).length > 0) {
                        finalBodyStr = JSON.stringify(req.body);
                    }
                    
                    if (finalBodyStr) {
                        finalBodyBuffer = Buffer.from(finalBodyStr, 'utf8');
                        fetchOptions.body = finalBodyBuffer;
                        fetchOptions.headers['Content-Length'] = finalBodyBuffer.length.toString();
                        
                        // FAST-TRACK: Prevents OpenCode from timing out waiting for NVIDIA
                        if (finalBodyStr.includes('"stream":true') || finalBodyStr.includes('"stream": true')) {
                            if (!res.headersSent) {
                                res.writeHead(200, {
                                    'Content-Type': 'text/event-stream; charset=utf-8',
                                    'Cache-Control': 'no-cache',
                                    'Connection': 'keep-alive',
                                    'Transfer-Encoding': 'chunked',
                                    'X-Accel-Buffering': 'no',
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Headers': '*'
                                });
                                res.write(':\\n\\n'); // Envia um heartbeat vazio em formato SSE para iniciar o stream sem quebrar o cliente
                                console.log(`[Proxy ⚡] Fast-Track: Headers 200 OK enviados imediatamente para evitar timeout do OpenCode!`);
                            }
                        }
                    }
                }
            }

            console.log(`[Proxy 🚀] Enviando fetch para: ${targetUrl} | Usando chave: ...${keyObj.key.slice(-4)}`);
            var startTime = Date.now(); // Declarado com var para ser acessível no catch
            
            fetchOptions.signal = controller.signal;
            const fetchRes = await fetch(targetUrl, fetchOptions);
            const duration = Date.now() - startTime;
            
            req.removeListener('aborted', onClientDisconnect);

            console.log(`[Proxy ✅] Resposta da NVIDIA recebida em ${duration}ms! Status: ${fetchRes.status}`);

            if (fetchRes.status === 429) {
                keyCooldowns.set(keyObj.id, Date.now() + COOLDOWN_MS);
                console.warn(`[Proxy ⚠️] Rate limit (429) na chave '${keyObj.name}'. Colocada em cooldown por ${COOLDOWN_MS / 1000}s. Realizando fallback para a próxima chave...`);
                continue;
            }

            if (fetchRes.status !== 200) {
                console.error(`[Proxy ❌] Erro da NVIDIA (HTTP ${fetchRes.status}) com a chave '${keyObj.name}'`);
                const errorText = await fetchRes.text();
                console.error(`[Proxy ❌] Detalhes do Erro:`, errorText);
                
                if (res.headersSent) {
                    // Já iniciamos o Fast-Track SSE. Temos que mandar o erro como se fosse uma resposta do modelo,
                    // ou no formato de erro da OpenAI para que o OpenCode exiba na tela e não pare "do nada".
                    res.write(`data: {"error": {"message": ${JSON.stringify(errorText)}}}\n\n`);
                    res.end();
                } else {
                    res.status(fetchRes.status).send(errorText);
                }
                return;
            }

            const excludeHeaders = ['content-encoding', 'content-length', 'transfer-encoding', 'connection'];
            fetchRes.headers.forEach((val, key) => {
                if (!excludeHeaders.includes(key.toLowerCase())) {
                    if (!res.headersSent) {
                        res.setHeader(key, val);
                    }
                }
            });

            if (!res.headersSent) {
                res.status(fetchRes.status);
            }

            if (fetchRes.body) {
                console.log(`[Proxy 🌊] Iniciando streaming de altíssima eficiência (Zero-Buffer) para o cliente...`);
                const reader = fetchRes.body.getReader();
                
                const pump = async () => {
                    try {
                        while (true) {
                            const { done, value } = await reader.read();
                            if (done) break;
                            res.write(value);
                        }
                        console.log(`[Proxy ✨] Streaming finalizado com sucesso!`);
                    } catch (err) {
                        console.error(`[Proxy ❌] Erro no stream com a chave '${keyObj.name}':`, err.message);
                        if (!res.headersSent) res.status(500);
                    } finally {
                        res.end();
                    }
                };
                pump();
                return;
            } else {
                console.log(`[Proxy ✨] Resposta sem body. Finalizando req.`);
                return res.end();
            }

        } catch (error) {
            req.removeListener('aborted', onClientDisconnect);
            const duration = typeof startTime !== 'undefined' ? Date.now() - startTime : 0;
            if (error.name === 'AbortError' || clientClosed) {
                console.warn(`[Proxy ⚠️] Fetch cancelado (AbortError). O cliente (OpenCode) desconectou após ${duration}ms aguardando. Erro: ${error.message}`);
                return; // Já respondemos ou o cliente abortou
            }
            console.error(`[Proxy ❌] Erro ao conectar na API da NVIDIA após ${duration}ms com a chave '${keyObj.name}':`, error.message);
            continue; 
        }
    }

    console.error(`[Proxy 🛑] Todas as chaves do pool falharam.`);
    res.status(429).json({ error: "Rate limit atingido em todas as chaves da Nvidia NIM disponíveis." });
});

proxyApp.listen(11431, () => {
    console.log('✅ Proxy NVIDIA NIM rodando na porta 11431');
});