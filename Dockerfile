# Usa Node 20 por ter suporte nativo aprimorado a fetch e Web Streams
FROM node:20-alpine

WORKDIR /app

# Copia dependências primeiro para cache eficiente do Docker
COPY package*.json ./
RUN npm install

# Copia o resto do código
COPY . .

# Exposição das portas solicitadas
EXPOSE 88
EXPOSE 11431

# Comando de inicialização
CMD ["npm", "start"]