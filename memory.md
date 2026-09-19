# Memoria del proyecto — build-your-kafka

Contexto persistente para retomar el proyecto en cualquier sesión futura (humana o de Claude Code). Última actualización: 2026-09-19.

## Qué es esto

Clon simplificado de Apache Kafka en Java, siguiendo la serie de artículos "Build Your Own Kafka" (buildthingsuseful, Medium). Empezó como proyecto personal de aprendizaje y ahora también es la entrega de la materia **Sistemas Distribuidos** — individual, metodología **Kanban** exigida por la cátedra.

Repo: `github.com/Chillipeeper1/build-your-kafka` — package base `com.simplekafka.broker`.

## Fuentes del artículo guía

| Etapa | Tema | Link |
|---|---|---|
| 3 | Cliente de ZooKeeper | https://buildthingsuseful.medium.com/stage-3-build-your-own-kafka-3baaf4c873de |
| 4 | Capa de almacenamiento | https://buildthingsuseful.medium.com/stage-4-build-your-own-kafka-building-the-storage-layer-7a8497a614f3 |
| 5 | El broker | https://buildthingsuseful.medium.com/stage-5-build-your-own-kafka-building-the-broker-e516c4757356 |
| 6 | Librería cliente | https://buildthingsuseful.medium.com/stage-6-build-your-own-kafka-developing-the-client-library-96ecc0218089 |
| 7 | APIs de producer/consumer | https://buildthingsuseful.medium.com/stage-7-build-your-own-kafka-building-higher-level-producer-and-consumer-apis-2fba12ecf062 |
| 8 | Pruebas del sistema | https://buildthingsuseful.medium.com/stage-8-build-your-own-kafka-test-the-system-39ca1c1dca5e |

Medium bloquea el fetch directo (paywall/bot-blocking); funciona pasando la URL por `https://r.jina.ai/<url>`.

## Estado actual: capa de control (Etapa 3) — completa y verificada

**`ZooKeeperClient`**
- `connect()` — conexión asíncrona con patrón `Watcher` + `CountDownLatch`, crea `/brokers`, `/brokers/ids`, `/topics` como nodos persistentes.
- `createPersistentNode(path, data)` / `createEphemeralNode(path, data)` — creación genérica; la efímera devuelve `boolean` (si ganaste la carrera por crearla).
- `watchChildren(path, callback)` / `watchNode(path, callback)` — watches reactivos con re-armado recursivo (un watch de ZooKeeper es de un solo disparo).
- `process(WatchedEvent)` — maneja `SyncConnected` (destraba el latch), `Disconnected` (no-op, la librería reconecta sola), `Expired` (recrea el objeto `ZooKeeper` + el latch).

**`Broker`**
- `start()` → `connect()` + `registerBroker()` + `electController()`.
- `registerBroker()` — nodo efímero en `/brokers/ids/<id>` con `host:puerto`.
- `electController()` — compite por `/controller` (efímero); el que pierde queda con `watchNode` esperando la vacante.

**`BrokerInfo`** — record simple (id, host, puerto). **`Protocol`** — vacío, es lo próximo.

Todo esto se probó en vivo contra un ZooKeeper real en Docker (`docker run -d --name zookeeper -p 2181:2181 zookeeper`), incluyendo una carrera real de 2 brokers por la elección de controller.

## Bugs de integración ya resueltos (no repetirlos)

1. `connect()` NO debe crear `/controller` como nodo persistente — rompe la elección, porque `createEphemeralNode` ve que ya existe y nadie gana nunca. `/controller` es el premio efímero en sí mismo, no una carpeta.
2. `/brokers/ids` sí necesita crearse como padre persistente en `connect()` — ZooKeeper no crea un hijo si el padre no existe (no es como `mkdir -p`).
3. Forzar un `Expired` real de sesión con `docker restart`/`stop`+`start` **no funcionó** ni con 8s de caída — el server persiste la sesión en su log de transacciones y el cliente reconecta directo (`Disconnected` → `SyncConnected`). Para verlo de verdad haría falta la técnica de "session hijack" (abrir un segundo cliente con el mismo sessionId+password y cerrarlo).

## Etapa 4 — capa de almacenamiento: conceptos a implementar (todavía no escrita, arrancamos ahora)

Artículo releído completo el 2026-09-19. Antes de tocar código, esto es lo que hay que llevar a `Partition` y por qué. Referencia rápida para no reinventar el diseño a mitad de camino.

**Clase `Partition` — campos:**
- `id` (int) — identificador de la partición.
- `leader` (int) + `followers` (List<Integer>) — modelo líder-seguidor a nivel de partición (cada partición tiene su propio líder, no el broker entero). La sincronización real entre ellos queda para más adelante; acá solo se guarda quién es quién.
- `baseDir` (String) — carpeta en disco donde viven los segmentos de esta partición.
- `nextOffset` (AtomicLong) — próximo offset a asignar. Atomic porque el offset es el único estado compartido entre hilos que no está protegido por el lock de I/O.
- `lock` (ReadWriteLock) — múltiples lectores concurrentes, un solo escritor exclusivo. Evita que una lectura vea el índice a medias mientras un append lo está actualizando, sin pagar el costo de serializar todas las lecturas entre sí.
- `activeLogFile` (RandomAccessFile) + `activeLogChannel` (FileChannel) — el segmento actualmente abierto para escribir. Solo uno puede estar activo a la vez.
- `segments` (List<SegmentInfo>) — metadata en memoria de todos los segmentos de la partición (log + index ya creados), ordenada por offset base.

**Segmentación del log:**
- Un segmento rota a 1MB. Al llegar al límite, se cierra y se abre uno nuevo — así el log de una partición nunca es un archivo único gigante (facilita borrado/retención futura y recovery rápido, porque solo hay que re-escanear el segmento activo, no todo el histórico).
- Nombre de archivo: offset base con padding a 20 dígitos (`00000000000000000000.log`). El padding fijo permite que un listado de directorio por orden alfabético sea también el orden temporal correcto — no hace falta parsear todo para saber el orden.
- Cada segmento es un par `.log` (los mensajes) + `.index` (offset→posición dentro de ese log). Mismo nombre base, dos extensiones.

**Formato del mensaje en el `.log`:**
- Prefijo de 4 bytes con la longitud del mensaje, seguido del payload. Permite lectura secuencial (leer longitud, saltar esa cantidad de bytes, repetir) sin tener que cargar el segmento entero en memoria para saber dónde termina cada mensaje.

**Índice offset→posición:**
- Cada entrada: 8 bytes offset + 8 bytes posición = 16 bytes fijos por entrada. Tamaño fijo permite acceso aleatorio directo (`posición_en_index = offset_relativo * 16`) en vez de tener que escanear el índice.
- Es un **sparse index**: no se indexa cada mensaje individual (eso ahorra memoria/disco a costa de que una búsqueda puede necesitar un pequeño scan secuencial extra desde la entrada indexada más cercana). Si el offset buscado no está indexado exactamente, se usa la entrada anterior más próxima y se escanea desde ahí.
- Se actualiza en cada `append`, no de forma diferida, y se fuerza a disco con `force(true)` junto con el log — la garantía de durabilidad es "si `append` devolvió el offset, ya está en disco", no "eventualmente".

**`append(byte[] message) → long`:**
1. Write lock exclusivo.
2. Si el segmento activo ya pasó 1MB, rota (crea uno nuevo y lo abre).
3. Escribe longitud (4 bytes) + payload al `FileChannel`, `force(true)`.
4. Agrega la entrada correspondiente al `.index`, `force(true)`.
5. Incrementa `nextOffset` y lo devuelve.

**`readMessages(long offset, int maxBytes) → List<byte[]>`:**
1. Read lock (no exclusivo — varias lecturas en simultáneo están bien).
2. Busca con **binary search** (O(log n)) en `segments` cuál contiene ese offset — por eso conviene mantener `segments` ordenada por offset base en memoria.
3. Dentro del segmento, calcula la posición exacta en el `.log` vía el `.index` (acceso directo, no scan lineal) y lee secuencialmente hasta `maxBytes`, cruzando al siguiente segmento si hace falta de forma transparente para quien llama.

**Recovery al arrancar (`initialize()`):**
- No se persiste `nextOffset` en ningún lado aparte. Al iniciar, se escanea el directorio, se parsean los offsets base de los nombres de archivo, se ordenan, y `nextOffset` se reconstruye a partir de lo que ya hay en disco. Menos metadata que mantener consistente = menos superficie para que un crash deje todo en un estado corrupto.

**Por qué `FileChannel` (NIO) y no streams tradicionales:** acceso más directo a las primitivas de I/O del SO, y `force(true)` da control explícito de cuándo se sincroniza a disco en vez de depender del buffering implícito de un stream.

**Lo que NO entra en esta etapa** (queda para etapas 5+): sincronización real leader→followers (acá solo se guarda la lista, no hay red todavía), server de red, y que `Broker` escriba metadata de topics bajo `/topics` en ZooKeeper.

## Qué falta (plano de datos — nada de esto existe todavía)

- Etapa 4: `Partition` — ver detalle completo arriba. **Próximo paso concreto de la sesión.**
- Etapa 5: `SimpleKafkaBroker` — servidor de red, produce/fetch/replicate. `Broker` todavía no escribe metadata de topics bajo `/topics` (el método existe, nadie lo llama para eso).
- Etapa 6: `SimpleKafkaClient` — descubrimiento de brokers + enrutamiento por partición.
- Etapa 7: `SimpleKafkaProducer` / `SimpleKafkaConsumer` sobre el cliente.
- Etapa 8: pruebas multi-broker con tolerancia a fallos real.

Hoy, mandar un mensaje es un no-op literal: no hay servidor de red, ni storage, ni nadie escribiendo metadata de topics.

## Cómo se trabaja en este proyecto (estilo de colaboración)

El dueño del proyecto está **aprendiendo** sistemas distribuidos construyendo, no shippeando — escribe el código de cada clase él mismo; Claude revisa, compila (`mvn compile`) y señala bugs con la causa raíz explicada, sin escribir la lección por él. Arneses de prueba descartables (`Main.java`, `println` temporales) sí los escribe Claude directamente.

Verificación en vivo por sobre confiar solo en la lógica: contenedor de ZooKeeper local corriendo, classpath con `mvn dependency:build-classpath -Dmdep.outputFile=cp.txt`, correr con `java -cp "target/classes;$(cat cp.txt)" com.simplekafka.broker.<Clase>`. `zkCli.sh` dentro del contenedor (`docker exec zookeeper bash -c "echo '<cmd>' | zkCli.sh -server localhost:2181"`) sirve para inspeccionar/mutar znodes a mano y simular otros miembros del cluster.

Cuando hay una decisión de arquitectura real (no solo una corrección), se le presentan las opciones al dueño del proyecto y decide él.

## Documento de propuesta académica

Ya generado: `propuesta-proyecto.pdf` en la raíz del repo (Objetivo, Justificación, Análisis de requisitos con 7 RF confirmados contra las fuentes, Metodología y roles — Kanban individual). No está commiteado al repo todavía, es decisión del dueño si lo versiona ahí o lo entrega directo por el portal de la materia.
