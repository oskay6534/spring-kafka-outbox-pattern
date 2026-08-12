# 🚀 Spring Boot & Apache Kafka — Transactional Outbox Pattern

Bu proje, **Spring Boot, Apache Kafka ve PostgreSQL** kullanılarak event-driven (olay güdümlü) bir sistemin temel çalışma mantığını öğrenmek ve uygulamak amacıyla geliştirilmiştir.

Projede yalnızca Kafka üzerinden mesaj gönderme ve tüketme işlemleri değil; dağıtık sistemlerde karşılaşılabilecek **mesaj kaybı** ve **aynı event'in birden fazla kez işlenmesi** gibi problemlere karşı kullanılan yaklaşımlar da uygulanmıştır.

Projede özellikle:

- Apache Kafka Producer / Consumer yapısı
- Kafka Topic, Partition, Offset ve Consumer Group
- Serialization / Deserialization
- Kafka Key kullanımı
- ACK ve Idempotent Producer
- Transactional Outbox Pattern
- Idempotent Consumer
- Spring `@KafkaListener`
- Spring `@Scheduled`
- PostgreSQL ve Spring Data JPA
- Transaction yönetimi

konuları üzerinde çalışılmıştır.

---

## 🛠 Kullanılan Teknolojiler

- Java 17
- Spring Boot
- Spring Kafka
- Spring Data JPA
- Apache Kafka
- PostgreSQL
- Docker
- Maven
- Jackson

---

# 📌 Projenin Temel Senaryosu

Sistemde bir sipariş oluşturulduğunda sipariş bilgisi PostgreSQL veritabanına kaydedilir.

Aynı zamanda diğer servislerin bu siparişin oluşturulduğunu öğrenebilmesi için bir:

```text
ORDER_CREATED
```

event'i oluşturulur.

Normalde aşağıdaki gibi bir yapı kurulabilir:

```text
Order oluştur
      ↓
Database'e kaydet
      ↓
Kafka'ya event gönder
```

Ancak burada önemli bir problem vardır.

Sipariş veritabanına başarıyla kaydedildikten hemen sonra Kafka erişilemez hale gelirse:

```text
Order DB'ye kaydedildi ✅

Kafka'ya event gönderilemedi ❌
```

durumu oluşabilir.

Böyle bir durumda sipariş sistemde bulunmasına rağmen diğer servisler siparişin oluşturulduğunu öğrenemez.

Bu problemi azaltmak amacıyla projede **Transactional Outbox Pattern** uygulanmıştır.

---

# 📦 Transactional Outbox Pattern

Sipariş oluşturulduğunda Kafka'ya doğrudan mesaj göndermek yerine hem sipariş hem de gönderilecek event aynı veritabanına ve aynı transaction içerisinde kaydedilir.

```text
              OrderService
                   ↓
            @Transactional
                   ↓
        ┌────────────────────┐
        │                    │
        │   Order            │
        │     ↓              │
        │   orders           │
        │                    │
        │   OutboxEvent      │
        │     ↓              │
        │   outbox_events    │
        │                    │
        └────────────────────┘
                   ↓
                 COMMIT
```

Bu sayede:

```text
Order kaydı başarılı
+
Outbox kaydı başarılı
        ↓
      COMMIT
```

olur.

İşlemlerden herhangi biri başarısız olursa:

```text
Order başarılı
+
Outbox başarısız
        ↓
     ROLLBACK
```

gerçekleşir.

Böylece:

```text
Order var
ama event yok
```

gibi tutarsız bir durumun oluşması engellenir.

---

# 📨 OrderCreatedEvent

Sistemde bir sipariş oluşturulduğunda bu olayı temsil etmek için `OrderCreatedEvent` oluşturulur.

Örnek olarak event şu bilgileri taşıyabilir:

```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "orderId": 1,
  "customerId": 15,
  "totalPrice": 250.75
}
```

Burada:

| Alan | Açıklama |
|---|---|
| `eventId` | Event'in benzersiz kimliği |
| `orderId` | Event'in ait olduğu sipariş |
| `customerId` | Siparişi oluşturan müşteri |
| `totalPrice` | Sipariş toplam tutarı |

`Order` entity'si veritabanındaki siparişi temsil ederken, `OrderCreatedEvent` sistemde gerçekleşen:

```text
"Sipariş oluşturuldu"
```

olayını temsil eder.

---

# 🗃 OutboxEvent

Oluşturulan `OrderCreatedEvent`, Jackson kullanılarak JSON formatına çevrilir ve `outbox_events` tablosunda saklanır.

Örnek bir Outbox kaydı:

```text
id             = event UUID
aggregateType  = ORDER
aggregateId    = 1
eventType      = ORDER_CREATED
payload        = {...JSON...}
status         = NEW
createdAt      = ...
```

Yeni oluşturulan event:

```text
status = NEW
```

durumunda bekler.

Bu, event'in henüz Kafka'ya başarıyla gönderilmediğini ifade eder.

---

# 📤 OutboxPublisher

`OutboxPublisher`, Kafka'ya henüz gönderilmemiş Outbox kayıtlarını bulmak ve Kafka'ya göndermekle sorumludur.

Spring'in:

```java
@Scheduled(fixedDelay = 5000)
```

özelliği kullanılarak Outbox tablosu belirli aralıklarla kontrol edilir.

Publisher:

```text
outbox_events
      ↓
status = NEW
      ↓
En eski kayıtlardan başlayarak
en fazla 100 event al
      ↓
Kafka'ya gönder
```

akışını uygular.

Kafka mesajı başarıyla kabul ettiğinde:

```text
NEW
 ↓
Kafka
 ↓
ACK
 ↓
SENT
```

Outbox kaydının durumu `SENT` olarak değiştirilir.

Kafka erişilemez durumdaysa event `NEW` olarak kalır.

```text
NEW
 ↓
Kafka'ya gönder
 ↓
Başarısız ❌
 ↓
NEW olarak kal
 ↓
Sonraki çalışmada tekrar dene
```

Böylece geçici Kafka veya ağ problemlerinde event bilgisi kaybolmaz.

---

# 🔑 Kafka Message Key ve Partition

Kafka'ya mesaj gönderilirken siparişin ID değeri message key olarak kullanılmaktadır.

Örneğin:

```text
key = orderId
```

Aynı key'e sahip mesajlar aynı partition'a yönlendirildiğinden aynı siparişle ilgili event'lerin sıralamasını korumak kolaylaşır.

Örneğin:

```text
Order 25 → ORDER_CREATED
Order 25 → ORDER_PAID
Order 25 → ORDER_SHIPPED

        ↓

key = "25"

        ↓

aynı partition
```

---

# ⚙️ Kafka Producer Akışı

Projede Kafka Producer tarafındaki temel akış:

```text
OutboxPublisher
      ↓
KafkaTemplate
      ↓
ProducerRecord
      ↓
Serializer
      ↓
Partition seçimi
      ↓
RecordAccumulator
      ↓
Batch
      ↓
Kafka Broker
      ↓
ACK
```

Producer tarafında String key ve JSON String payload kullanılmaktadır.

```properties
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
```

Ayrıca producer idempotence aktif edilmiştir:

```properties
spring.kafka.producer.properties.enable.idempotence=true
```

ve güvenli acknowledgement için:

```properties
spring.kafka.producer.acks=all
```

kullanılmıştır.

---

# 📥 Kafka Consumer Akışı

Kafka'dan gelen mesajların işlenmesi `OrderConsumer` tarafından gerçekleştirilir.

```text
Kafka Broker
      ↓
KafkaConsumer
      ↓
poll()
      ↓
Spring Listener Container
      ↓
Deserializer
      ↓
OrderCreatedEvent
      ↓
@KafkaListener
      ↓
OrderConsumer.consume()
```

Spring Kafka, Kafka Consumer altyapısını ve Listener Container'ı yönetir.

`@KafkaListener` ile işaretlenen `consume()` metodu doğrudan Kafka'dan mesaj çeken `KafkaConsumer` nesnesi değildir.

Mesaj Kafka'dan alındıktan ve deserialize edildikten sonra Spring tarafından iş mantığını gerçekleştirmek üzere çağrılan listener metodudur.

---

# 🔄 Serialization ve Deserialization

Producer tarafında Outbox payload zaten JSON String olarak tutulduğu için:

```text
JSON String
     ↓
StringSerializer
     ↓
byte[]
     ↓
Kafka
```

işlemi gerçekleşir.

Consumer tarafında ise:

```text
Kafka byte[]
     ↓
JacksonJsonDeserializer
     ↓
OrderCreatedEvent
```

dönüşümü yapılır.

Bu sayede listener metodunda doğrudan:

```java
public void consume(OrderCreatedEvent event)
```

kullanılabilir.

---

# 👥 Consumer Group

Consumer:

```text
order-group
```

isimli Consumer Group içerisinde çalışmaktadır.

```properties
spring.kafka.consumer.group-id=order-group
```

Birden fazla consumer instance aynı group içerisinde çalıştırıldığında Kafka partition'ları consumer'lar arasında dağıtabilir.

---

# 📍 Offset Yönetimi

Projede:

```properties
spring.kafka.consumer.auto-offset-reset=earliest
```

kullanılmıştır.

Consumer Group için geçerli commit edilmiş bir offset bulunmuyorsa consumer mevcut en eski mesajdan okumaya başlar.

```text
Commit edilmiş offset var
        ↓
Kaldığı yerden devam

Commit edilmiş offset yok
        ↓
earliest
        ↓
En eski mevcut mesajdan başla
```

---

# 🛡 Idempotent Consumer

Kafka sistemlerinde aynı event'in birden fazla kez teslim edilme ihtimaline karşı consumer tarafında idempotency uygulanmıştır.

Her event benzersiz bir:

```text
eventId
```

taşır.

Consumer mesajı aldığında önce:

```text
processed_events
```

tablosunu kontrol eder.

```text
Event geldi
     ↓
eventId daha önce işlendi mi?
     ↓
   ┌───────┴───────┐
   │               │
 EVET            HAYIR
   │               │
   ↓               ↓
Atla          İş mantığını çalıştır
                   ↓
             ProcessedEvent kaydet
```

Eğer aynı `eventId` daha önce işlenmişse event tekrar işlenmez.

Bu yaklaşım özellikle ödeme, stok azaltma veya başka bir veritabanı işleminin aynı event nedeniyle birden fazla kez gerçekleştirilmesini engellemek için önemlidir.

---

# 🔐 Transaction Yönetimi

Hem producer/outbox oluşturma tarafında hem de consumer işleme tarafında transaction yönetimi kullanılmaktadır.

Sipariş oluşturma tarafında:

```java
@Transactional
public Long createOrder(...) {
    // Order kaydı
    // Outbox kaydı
}
```

Amaç:

```text
Order
+
OutboxEvent
```

kayıtlarının birlikte başarılı veya başarısız olmasıdır.

Consumer tarafında ise iş mantığı ile `ProcessedEvent` kaydının aynı transaction içerisinde tutulması hedeflenmiştir.

```text
İş mantığı başarılı
+
ProcessedEvent başarılı
        ↓
      COMMIT
```

Bir DB işlemi başarısız olursa:

```text
ROLLBACK
```

gerçekleşir.

---

# 🏗 Genel Mimari

Projenin temel uçtan uca akışı:

```text
                 HTTP Request
                      ↓
                OrderController
                      ↓
                 OrderService
                      ↓
                @Transactional
                      ↓
          ┌─────────────────────┐
          │                     │
          │ Order → orders      │
          │                     │
          │ OutboxEvent         │
          │ status = NEW        │
          │       ↓             │
          │ outbox_events       │
          │                     │
          └─────────────────────┘
                      ↓
                    COMMIT

=================================================

                OutboxPublisher
                      ↓
                NEW event'leri bul
                      ↓
                 KafkaTemplate
                      ↓
                 Apache Kafka
                      ↓
                     ACK
                      ↓
              Outbox → SENT

=================================================

                 Apache Kafka
                      ↓
                 KafkaConsumer
                      ↓
               Listener Container
                      ↓
                 Deserializer
                      ↓
              OrderCreatedEvent
                      ↓
                OrderConsumer
                      ↓
             Event işlendi mi?
                 /         \
              EVET         HAYIR
               ↓             ↓
             Atla       İş mantığı
                             ↓
                     ProcessedEvent
                             ↓
                        PostgreSQL
```

---

# 📂 Temel Sınıflar

Projede kullanılan temel sınıfların sorumlulukları:

| Sınıf | Sorumluluk |
|---|---|
| `OrderController` | HTTP isteklerini karşılar |
| `OrderService` | Sipariş ve Outbox kaydını oluşturur |
| `Order` | Sipariş veritabanı entity'si |
| `OrderCreatedEvent` | Sipariş oluşturulma olayını temsil eder |
| `OutboxEvent` | Kafka'ya gönderilecek event'in Outbox kaydı |
| `OutboxPublisher` | NEW Outbox kayıtlarını Kafka'ya gönderir |
| `OrderConsumer` | Kafka'dan gelen event'i işler |
| `ProcessedEvent` | Daha önce işlenen event'leri takip eder |
| `OrderRepository` | Order veritabanı işlemleri |
| `OutboxEventRepository` | Outbox veritabanı işlemleri |
| `ProcessedEventRepository` | İşlenmiş event veritabanı işlemleri |

---

# ▶️ Projeyi Çalıştırma

## Gereksinimler

Projeyi çalıştırmak için:

- Java 17
- Maven
- PostgreSQL
- Apache Kafka
- Docker

gereklidir.

PostgreSQL üzerinde:

```text
kafka_db
```

isimli bir database oluşturulmalıdır.

---

## 🔐 Veritabanı Şifresi

Veritabanı şifresi güvenlik nedeniyle repository içerisinde tutulmamaktadır.

`application.properties`:

```properties
spring.datasource.username=${DB_USERNAME:postgres}
spring.datasource.password=${DB_PASSWORD}
```

Windows PowerShell üzerinde uygulamayı çalıştırmadan önce:

```powershell
$env:DB_PASSWORD="postgres-sifreniz"
```

tanımlanabilir.

İstenirse kullanıcı adı da:

```powershell
$env:DB_USERNAME="postgres"
```

şeklinde tanımlanabilir.

---

## Kafka

Kafka broker'ın:

```text
localhost:9092
```

adresinden erişilebilir olması beklenmektedir.

```properties
spring.kafka.bootstrap-servers=localhost:9092
```

Kafka Docker üzerinden çalıştırılıyorsa uygulama başlatılmadan önce ilgili container'ların çalıştığından emin olunmalıdır.

---

# 🎯 Bu Projede Öğrenilen Temel Kavramlar

Bu proje kapsamında özellikle aşağıdaki konular uygulamalı olarak incelenmiştir:

- Event-Driven Architecture
- Apache Kafka Producer
- Apache Kafka Consumer
- Topic
- Partition
- Offset
- Message Key
- Consumer Group
- ProducerRecord
- RecordAccumulator
- Kafka batching
- Serialization / Deserialization
- ACK
- Producer Idempotence
- Spring Kafka
- KafkaTemplate
- `@KafkaListener`
- Listener Container
- Transactional Outbox Pattern
- Idempotent Consumer
- Transaction yönetimi
- Spring Data JPA
- PostgreSQL

---

## 📌 Projenin Amacı

Bu repository production seviyesinde tamamlanmış bir e-ticaret sistemi olmaktan ziyade, **Apache Kafka'nın Spring Boot uygulamalarında nasıl çalıştığını ve güvenilir event işleme yaklaşımlarının nasıl uygulanabileceğini öğrenmek amacıyla geliştirilmiş bir çalışma projesidir.**

Özellikle sadece Kafka API'lerini kullanmak yerine, kullanılan yapıların arkasındaki:

```text
"Neden buna ihtiyaç var?"
```

sorusunun anlaşılması hedeflenmiştir.