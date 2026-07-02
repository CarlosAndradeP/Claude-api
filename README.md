# NIM Proxy Load Balancer

Proxy local para balanceamento de chaves de API do NVIDIA NIM, projetado para contornar o limite de 40 RPM (requisições por minuto) por chave através de roteamento inteligente e fallback automático entre múltiplas chaves.

## Funcionalidades

- **Balanceamento de chaves**: Distribui requisições entre um pool de chaves NVIDIA NIM com roteamento baseado no tipo de agente
- **Fallback automático com cooldown**: Ao receber HTTP 429 (rate limit), tenta a próxima chave disponível no pipeline e coloca a chave falha em cooldown por 65 segundos, impedindo que ela seja reutilizada até o limite da NVIDIA resetar
- **Streaming SSE com Fast-Track**: Para requisições com `stream: true`, envia headers `200 OK` e heartbeat imediatamente, evitando timeout do cliente enquanto aguarda a resposta da NVIDIA
- **Zero-buffer streaming**: Replica os chunks do upstream diretamente para o cliente em tempo real
- **Modo Transparente**: Desabilita todas as modificações de body e headers Fast-Track, encaminhando a requisição exatamente como recebida (útil para debug)
- **Controle de Abort**: Configura se requisições upstream devem ser canceladas quando o cliente desconecta
- **Patch automático para GLM-5.1**: Limita `max_tokens` a 16384 e adiciona `temperature` e `top_p` padrão quando ausentes
- **Painel Admin Web**: Interface para gerenciamento de chaves em tempo real sem reiniciar o serviço
- **Suporte CORS**: Headers permissivos para acesso de qualquer origem

## Arquitetura

```
Cliente (OpenCode / LangChain / AutoGen)
        │
        ▼
┌─────────────────────────────────┐
│  Proxy Server (porta 11431)     │
│  - Intercepta /v1/*            │
│  - Injeta chave de API          │
│  - Roteamento por x-agent-type │
│  - Fallback em HTTP 429        │
│  - Streaming SSE                │
└──────────┬──────────────────────┘
           │
           ▼
  NVIDIA NIM API (integrate.api.nvidia.com)

┌─────────────────────────────────┐
│  Admin Server (porta 88)        │
│  - Painel Web de gestão         │
│  - API REST para chaves         │
│  - Configuração do proxy        │
└─────────────────────────────────┘
```

## Roteamento de Chaves (Pipeline)

O proxy roteia as requisições com base no header HTTP customizado `x-agent-type`:

| Header `x-agent-type` | Comportamento |
|---|---|
| `main` | Tenta chaves "Principal" primeiro, fallback para "Sub" e "Geral" |
| `sub` | Round-robin entre chaves "Sub", fallback para "Principal" e "Geral" |
| Ausente / `none` | Round-robin entre todas as chaves disponíveis |

Chaves duplicadas são removidas automaticamente antes de tentar o pipeline. Se todas as chaves forem exauridas, retorna HTTP 429 ao cliente.

## Instalação e Execução

### Docker (recomendado)

```bash
docker-compose up -d --build
```

### Node.js (desenvolvimento local)

Requer **Node.js 20+** (necessário para `fetch` nativo e Web Streams API).

```bash
npm install
npm start
```

## Configuração Pós-Instalação

1. Acesse `http://localhost:88` no navegador
2. Adicione suas chaves NVIDIA API (formato `nvapi-...`), atribuindo um papel:
   - **Principal** (`main`): Chave primária, usada primeiro pelo tipo `main`
   - **Sub-Agente** (`sub`): Chave secundária, usada em rodízio pelo tipo `sub`
   - **Geral** (`general`): Disponível como fallback para qualquer tipo
3. Configure seu cliente de IA para usar `http://localhost:11431/v1` como base URL

## Integração com OpenCode

Exemplo de configuração no `opencode.jsonc`:

```jsonc
{
  "model": "nim-proxy/z-ai/glm-5.1",
  "provider": {
    "nim-proxy": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "NIM Local Proxy",
      "options": {
        "baseURL": "http://localhost:11431/v1"
      },
      "models": {
        "z-ai/glm-5.1": {
          "name": "Z.AI GLM 5.1"
        }
      }
    }
  }
}
```

## API REST (Admin - Porta 88)

| Método | Endpoint | Descrição |
|---|---|---|
| `GET` | `/` | Painel admin web |
| `GET` | `/api/keys` | Retorna todas as chaves cadastradas |
| `POST` | `/api/keys` | Adiciona nova chave. Body: `{key, role, name}` |
| `DELETE` | `/api/keys/:id` | Remove uma chave pelo ID |
| `GET` | `/api/config` | Retorna configuração do proxy |
| `POST` | `/api/config` | Atualiza configuração. Body: `{disableAbort?, transparentMode?}` |

## Proxy (Porta 11431)

| Método | Endpoint | Descrição |
|---|---|---|
| `ALL` | `/v1/*` | Proxy reverso para `https://integrate.api.nvidia.com/v1/*` |

## Configurações do Proxy

| Configuração | Tipo | Padrão | Descrição |
|---|---|---|---|
| `disableAbort` | boolean | `false` | Quando `true`, não cancela requisições upstream ao desconectar o cliente |
| `transparentMode` | boolean | `true` | Quando `true`, desabilita modificações de body e headers Fast-Track (ativo por padrão) |

> **Nota**: Estas configurações são mantidas apenas em memória. O `transparentMode` reinicia sempre ativado (`true`); o `disableAbort` reinicia como `false`. O cooldown de 65s das chaves após 429 também é em memória e zera a cada reinício (esperado — o limite já terá resetado).

## Estrutura do Projeto

```
├── server.js              # Aplicação principal (Express, servidor duplo)
├── public/
│   └── index.html          # Painel admin web (HTML + Tailwind CSS)
├── Dockerfile              # Container Node.js 20 Alpine
├── docker-compose.yml      # Orquestração Docker
├── package.json            # Manifesto do projeto
├── data/
│   └── keys.json           # Armazenamento persistente das chaves (Docker volume)
├── Plano.md                # Documentação de arquitetura
└── example.md              # Exemplo de chamada direta à API NVIDIA NIM
```

## Dependências

- **express** `^4.19.2` - Framework web (única dependência de runtime)
- **Node.js 20+** - Requerido para `fetch` nativo, `AbortController` e Web Streams API
- **Docker** + **Docker Compose** - Para deploy containerizado

## Exemplo de Uso Direto

```javascript
import OpenAI from 'openai';

const openai = new OpenAI({
  apiKey: 'sua-chave-aqui',
  baseURL: 'http://localhost:11431/v1',
});

const completion = await openai.chat.completions.create({
  model: "z-ai/glm-5.1",
  messages: [{ role: "user", content: "Hello!" }],
  stream: true,
});

for await (const chunk of completion) {
  process.stdout.write(chunk.choices[0]?.delta?.content || '');
}
```

## Licença

MIT
