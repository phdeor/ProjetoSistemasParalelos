# ==========================================
# ESTÁGIO 1: Build (Compilação com Maven)
# ==========================================
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /app

# Copia os arquivos de configuração do Maven primeiro (aproveita o cache de dependências)
COPY pom.xml .
RUN mvn dependency:go-offline

# Copia o código-fonte restante
COPY src ./src

# Compila o projeto e empacota em um arquivo JAR executável
RUN mvn clean package -DskipTests

# ==========================================
# ESTÁGIO 2: Execução (Apenas Runtime leve)
# ==========================================
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# Copia apenas o arquivo .jar gerado no estágio anterior para a imagem final
COPY --from=builder /app/target/bbs-chat-1.0-SNAPSHOT.jar app.jar

# Como o seu projeto possui duas classes principais (ServerMain e ClientBotMain),
# deixamos o comando padrão para iniciar o Servidor. 
# (Você pode sobrescrever isso no docker-compose se quiser rodar o bot).
CMD ["java", "-cp", "app.jar", "com.sistema.ServerMain"]