# NIM Proxy Load Balancer

Proxy local para múltiplas chaves da API NVIDIA NIM, com balanceamento consciente de capacidade, fallback limitado, streaming e painel de saúde/uso.

## O que mudou no balanceamento

O proxy não faz mais um round-robin cego nem tenta o pool inteiro em toda falha. Cada chave possui estado próprio:

- **Token bucket**: padrão de 38 RPM por chave, abaixo do limite nominal de 40 RPM.
- **Burst limitado**: no máximo 3 chamadas imediatas por chave.
- **Concorrência limitada**: no máximo 2 chamadas em voo por chave.
- **Menor carga primeiro**: dentro do papel preferido, escolhe a chave com menos chamadas em voo, mais tokens e uso menos recente.
- **Máximo de tentativas**: uma chamada tenta no máximo 3 chaves, evitando amplificação de carga.
- **Cooldown adaptativo**: `429` respeita `Retry-After`; sem esse header, começa em 65 segundos e cresce em falhas consecutivas.
- **Circuit breaker**: `401` e `403` com indicação explícita de credencial inválida isolam a chave por 15 minutos; falhas de rede e `5xx` usam backoff exponencial curto.
- **Restrição por modelo**: um `403` de permissão bloqueia apenas a combinação chave/modelo por 5 minutos. Outros modelos continuam usando aquela chave. Uma verificação autenticada pode recuperar o acesso antes desse prazo.
- **Cooldown protegido**: sucessos de chamadas paralelas antigas não apagam `Retry-After` nem circuitos abertos por falhas mais recentes.
- **Fila curta e limitada**: até 100 chamadas podem aguardar até 3 segundos por capacidade, sem reservar uma chave enquanto esperam. Cooldowns maiores são devolvidos imediatamente com `Retry-After`.
- **Backpressure no stream**: o proxy espera o cliente consumir cada chunk, em vez de acumular dados em memória.

O header `x-agent-type` mantém a afinidade:

| Valor | Preferência | Fallback |
|---|---|---|
| `main` | `main` e `all` | `sub` |
| `sub` | `sub` e `all` | `main` |
| ausente | qualquer chave | — |

## Estatísticas e saúde

O painel em `http://localhost:88` mostra, por chave:

- tentativas, sucessos, falhas e ocorrências de rate limit;
- taxa de sucesso, latência média e bytes de entrada/saída;
- chamadas atualmente em voo e capacidade estimada pelo token bucket;
- data do último uso, sucesso e erro;
- estado `Saudável`, `Falhando`, `Cooldown`, `Inativa` ou `Não testada`.

Toda inferência autenticada atualiza a saúde passivamente. O botão de coração executa um check individual em `POST /v1/chat/completions`, com `max_tokens: 1` e o modelo configurado no painel (`HEALTHCHECK_MODEL` define o valor inicial). Esse check **consome cota**. O catálogo público `/v1/models` não comprova que uma chave é válida e não marca mais uma chave como saudável.

Checks usam o mesmo limite local, não rodam simultaneamente para a mesma chave e podem reabilitar uma chave isolada por autenticação. Eles respeitam cooldowns ativos. Erros `400`/`404` contam como falhas da requisição, sem invalidar a chave para outros modelos. O painel distingue a última inferência autenticada e mostra quantos modelos têm restrição temporária.

As estatísticas ficam em `data/keys.json` e são gravadas de forma atômica e agrupada, inclusive sob carga contínua. A API administrativa nunca devolve o segredo completo; apenas a chave mascarada. Erros upstream também têm a chave usada removida antes de serem devolvidos ou armazenados.

## Execução

Requer Node.js 20 ou superior.

```bash
npm install
npm test
npm start
```

Com Docker:

```bash
docker compose up -d --build
```

Por padrão, o Compose publica as duas portas apenas em `127.0.0.1`, impedindo acesso acidental pela rede local. O diretório `data` não entra na imagem Docker.

Depois:

1. Acesse `http://localhost:88` e cadastre as chaves.
2. Configure o cliente com a base URL `http://localhost:11431/v1`.
3. Opcionalmente envie `x-agent-type: main` ou `x-agent-type: sub`.

## Configuração

Os limites podem ser alterados no painel. Variáveis de ambiente definem os valores iniciais:

| Variável | Padrão | Descrição |
|---|---:|---|
| `REQUESTS_PER_MINUTE` | `38` | Limite local por chave |
| `KEY_BURST` | `3` | Capacidade imediata por chave |
| `MAX_CONCURRENT_PER_KEY` | `2` | Chamadas simultâneas por chave |
| `MAX_ATTEMPTS` | `3` | Máximo de chaves tentadas por requisição |
| `QUEUE_WAIT_MS` | `3000` | Espera máxima por capacidade; de 0 a 10000 ms. Zero desativa a espera |
| `MAX_QUEUE_LENGTH` | `100` | Limite da fila; de 0 a 1000. Zero desativa a fila |
| `HEALTHCHECK_MODEL` | `openai/gpt-oss-20b` | Modelo usado na inferência mínima para verificar uma chave |
| `REQUEST_START_TIMEOUT_MS` | `110000` | Prazo compartilhado entre fila e tentativas até receber os headers upstream |
| `UPSTREAM_TIMEOUT_MS` | `120000` | Timeout até o upstream responder com headers |
| `UPSTREAM_IDLE_TIMEOUT_MS` | igual ao timeout de headers | Timeout sem novos chunks após os headers; reinicia a cada chunk |
| `DATA_DIR` | `./data` | Diretório de chaves e estatísticas |
| `UPSTREAM_ORIGIN` | NVIDIA NIM | Origem alternativa, útil para testes |
| `ADMIN_PORT` | `88` | Porta do painel |
| `PROXY_PORT` | `11431` | Porta do proxy |
| `PROXY_CORS_ORIGIN` | vazio | Habilita CORS somente para a origem informada |

Alterações feitas no painel são validadas e persistidas atomicamente em `data/proxy-config.json`. Essa configuração salva prevalece sobre os valores iniciais do ambiente após reinício. Uma falha de gravação não aplica parcialmente a alteração. O volume `./data:/app/data` existente já preserva esse arquivo no Coolify/Docker.

Não habilite CORS com `*` em uma instalação local: uma página maliciosa aberta no navegador poderia gerar chamadas contra o proxy e consumir as cotas.

As opções existentes continuam disponíveis:

- `transparentMode`: encaminha o body sem alterações. Desativado, aplica o ajuste de parâmetros do GLM-5.1.
- `disableAbort`: mantém uma chamada upstream já iniciada mesmo quando o cliente desconecta. A espera na fila sempre é cancelada quando o cliente sai, pois nenhuma geração começou.

O antigo “Fast-Track” foi removido. Enviar `200 OK` antes da resposta do upstream mascarava erros e tornava impossível fazer fallback HTTP corretamente. O streaming continua sendo repassado chunk a chunk assim que a NVIDIA aceita a requisição.

## API administrativa

| Método | Endpoint | Descrição |
|---|---|---|
| `GET` | `/api/keys` | Chaves mascaradas, estatísticas e saúde |
| `POST` | `/api/keys` | Adiciona `{ name, key, role }` |
| `DELETE` | `/api/keys/:id` | Exclui uma chave |
| `POST` | `/api/keys/:id/check` | Inferência autenticada mínima; aceita `{ "model": "publisher/model" }` opcional |
| `GET` | `/api/config` | Configuração atual |
| `POST` | `/api/config` | Atualiza opções e limites |

## Respostas de proteção

Quando a fila curta não consegue atender, o proxy responde `429` com `Retry-After` e `error.type = "proxy_capacity_exhausted"`. Isso protege as chaves contra uma fila sem limite e contra cascatas de fallback. A fila mantém a ordem entre chamadas elegíveis e não impede outro modelo de usar uma chave disponível.

Em erros de autenticação, rede, timeout, `429` ou `5xx`, outra chave elegível pode ser tentada. Erros de requisição como `400` ou `404` são devolvidos diretamente, pois repetir o mesmo payload em todas as chaves apenas multiplicaria a carga.

Depois de esgotar as alternativas, erros de credencial/permissão mantêm o status upstream `401`/`403`, evitando retentativas de rede inúteis pelo SDK. Um erro específico de modelo não é convertido em esgotamento global de capacidade. Respostas de sucesso, ferramentas e chunks SSE continuam sendo encaminhados sem alteração.

Streams interrompidos encerram a conexão com erro, preservando a indicação de resposta incompleta no cliente. Cancelamentos do cliente liberam a capacidade sem penalizar a saúde da chave. O `Retry-After` de esgotamento reflete o cooldown real do pool.

### Contrato com o Web Agent

Toda resposta inclui `x-request-id` (o cliente pode enviar seu próprio identificador). Erros acrescentam headers compatíveis com clientes que leem o body upstream original:

| Header | Exemplos |
|---|---|
| `x-proxy-error-type` | `proxy_capacity_exhausted`, `upstream_rate_limited`, `upstream_model_forbidden`, `upstream_authentication_error`, `upstream_timeout` |
| `x-proxy-error-scope` | `proxy`, `key`, `model_key`, `model`, `request`, `provider` |
| `x-proxy-retryable` | `true` para falhas temporárias; `false` para payload, permissão ou credencial inválida |
| `Retry-After` | Segundos até nova capacidade quando a resposta é `429` |

Erros gerados pelo proxy também incluem `scope`, `retryable` e `request_id` em `error`, preservando os campos anteriores. O cliente pode enviar `x-request-timeout-ms` para reduzir o prazo compartilhado de início; esse header não é encaminhado à NVIDIA. Depois dos headers upstream, o timeout continua sendo de **inatividade entre chunks**, sem cortar um stream saudável por atingir o prazo de início.

O Web Agent atual já entende `429` e `Retry-After`. Os novos campos permitem ao benchmark distinguir falha temporária de incompatibilidade do modelo em uma atualização do cliente. Eles não desativam nem reativam modelos automaticamente. O proxy não altera a disponibilidade comercial ou de preview dos modelos da NVIDIA.

## Verificação com o Web Agent

Na pasta `web-agent`, com as dependências dos dois projetos instaladas:

```bash
node --import tsx scripts/verify-proxy-integration.ts ../Claude-api
```

O script valida streaming, chamada de ferramenta, devolução do resultado ao modelo e propagação de interrupções usando o SDK do Web Agent e este proxy. Usa chaves fictícias, dados temporários e upstream local, sem consumir cota da NVIDIA.

## Segurança operacional

- `data/keys.json` contém segredos: mantenha-o fora do Git e faça backup seguro.
- Não publique a porta 88 na internet; o painel é administrativo e não possui login embutido.
- Para acesso remoto, coloque autenticação e TLS em um reverse proxy.
- Rode uma única instância por conjunto de chaves. Os limites são locais ao processo e não coordenam múltiplos containers.
