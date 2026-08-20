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
- **Circuit breaker**: `401`/`403` isolam a chave por 15 minutos; falhas de rede e `5xx` usam backoff exponencial curto.
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

Toda chamada real atualiza a saúde passivamente. O botão de coração executa um check ativo individual em `GET /v1/models`. Checks usam o mesmo limite local, não rodam simultaneamente para a mesma chave e podem reabilitar uma chave isolada.

As estatísticas ficam em `data/keys.json` e são gravadas de forma atômica e agrupada. A API administrativa nunca devolve o segredo completo; apenas a chave mascarada.

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
| `UPSTREAM_TIMEOUT_MS` | `120000` | Timeout até o upstream responder com headers |
| `UPSTREAM_ORIGIN` | NVIDIA NIM | Origem alternativa, útil para testes |
| `ADMIN_PORT` | `88` | Porta do painel |
| `PROXY_PORT` | `11431` | Porta do proxy |
| `PROXY_CORS_ORIGIN` | vazio | Habilita CORS somente para a origem informada |

Não habilite CORS com `*` em uma instalação local: uma página maliciosa aberta no navegador poderia gerar chamadas contra o proxy e consumir as cotas.

As opções existentes continuam disponíveis:

- `transparentMode`: encaminha o body sem alterações. Desativado, aplica o ajuste de parâmetros do GLM-5.1.
- `disableAbort`: mantém a chamada upstream mesmo quando o cliente desconecta.

O antigo “Fast-Track” foi removido. Enviar `200 OK` antes da resposta do upstream mascarava erros e tornava impossível fazer fallback HTTP corretamente. O streaming continua sendo repassado chunk a chunk assim que a NVIDIA aceita a requisição.

## API administrativa

| Método | Endpoint | Descrição |
|---|---|---|
| `GET` | `/api/keys` | Chaves mascaradas, estatísticas e saúde |
| `POST` | `/api/keys` | Adiciona `{ name, key, role }` |
| `DELETE` | `/api/keys/:id` | Exclui uma chave |
| `POST` | `/api/keys/:id/check` | Check ativo e individual |
| `GET` | `/api/config` | Configuração atual |
| `POST` | `/api/config` | Atualiza opções e limites |

## Respostas de proteção

Quando não há capacidade local, o proxy responde `429` com `Retry-After` e `error.type = "proxy_capacity_exhausted"`. Isso é intencional: rejeitar cedo protege as chaves contra uma fila sem limite e contra cascatas de fallback.

Em erros de autenticação, rede, timeout, `429` ou `5xx`, outra chave elegível pode ser tentada. Erros de requisição como `400` ou `404` são devolvidos diretamente, pois repetir o mesmo payload em todas as chaves apenas multiplicaria a carga.

## Segurança operacional

- `data/keys.json` contém segredos: mantenha-o fora do Git e faça backup seguro.
- Não publique a porta 88 na internet; o painel é administrativo e não possui login embutido.
- Para acesso remoto, coloque autenticação e TLS em um reverse proxy.
- Rode uma única instância por conjunto de chaves. Os limites são locais ao processo e não coordenam múltiplos containers.
