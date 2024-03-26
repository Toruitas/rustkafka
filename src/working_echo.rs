use tokio::time::{sleep, Duration};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use std::env;
use std::error::Error;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::net::{TcpListener, TcpStream};
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use tokio::sync::mpsc;
// use std::error::Error;
// use std::time::Duration;

// use rdkafka::config::ClientConfig;
// use rdkafka::producer::{FutureProducer, FutureRecord};
// use rdkafka::consumer::{Consumer, StreamConsumer};
// use rdkafka::message::Message;
// use rdkafka::error::KafkaResult;

// const BOOTSTRAP_SERVERS: &str = "localhost:9092";
// const TCP_IP: &str = "0.0.0.0";
// const TCP_PORT: u16 = 8888;

// async fn handle_connection(stream: TcpStream, producer: FutureProducer, consumer: StreamConsumer) -> Result<(), Box<dyn Error>> {
//     let mut buffer = [0; 1024];
//     let client_addr = stream.peer_addr()?.to_string();

//     println!("Incoming connection from: {}", client_addr);

//     // Generate unique topic name based on client address
//     let topic_name = format!("tcp_data_topic_{}", client_addr.replace(".", "_"));
//     println!("Using Kafka topic: {}", topic_name);

//     // Subscribe to the unique Kafka topic
//     consumer.subscribe(&[&topic_name])?;

//     let (tx, mut rx) = mpsc::channel::<String>(100);

//     // Spawn a task to handle Kafka consumer
//     tokio::spawn(async move {
//         while let Some(message) = consumer.poll(Duration::from_millis(100)).await {
//             match message {
//                 Ok(msg) => {
//                     if let Some(payload) = msg.payload() {
//                         if let Ok(data) = String::from_utf8(payload.to_vec()) {
//                             println!("Received message from Kafka: {}", data);
//                             // Send the Kafka message back to the TCP client
//                             if let Err(e) = tx.send(data).await {
//                                 eprintln!("Error sending to channel: {:?}", e);
//                                 break;
//                             }
//                         }
//                     }
//                 }
//                 Err(e) => eprintln!("Error while receiving from Kafka: {:?}", e),
//             }
//         }
//     });

//     // Read data from TCP stream and send to Kafka
//     tokio::spawn(async move {
//         loop {
//             let n = match stream_reader.read(&mut buffer).await {
//                 Ok(n) => n,
//                 Err(e) => {
//                     eprintln!("Error reading from TCP stream: {}", e);
//                     break;
//                 }
//             };

//             if n == 0 {
//                 println!("Connection closed by client: {}", client_addr);
//                 break;
//             }

//             let message = String::from_utf8_lossy(&buffer[..n]).into_owned();
//             println!("Received data from client {}: {}", client_addr, message);

//             // Produce the message to Kafka
//             let record = FutureRecord::to(&topic_name)
//                 .payload(&message)
//                 .key("key");

//             if let Err((e, _)) = producer.send(record).await {
//                 eprintln!("Error producing to Kafka: {:?}", e);
//                 break;
//             }
//         }
//     });

//     // Send data from Kafka back to TCP client
//     while let Some(message) = rx.recv().await {
//         if let Err(e) = stream.write_all(message.as_bytes()).await {
//             eprintln!("Error writing to TCP stream: {}", e);
//             break;
//         }
//     }

//     Ok(())
// }

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn Error>> {
//     let listener = TcpListener::bind(format!("{}:{}", TCP_IP, TCP_PORT)).await?;
//     println!("Listening on {}:{}", TCP_IP, TCP_PORT);

    

//     loop {
//         let (stream, _) = listener.accept().await?;

//         let producer = ClientConfig::new()
//             .set("bootstrap.servers", BOOTSTRAP_SERVERS)
//             .set("message.timeout.ms", "5000")
//             .create()
//             .expect("Producer creation error");

//         let consumer: StreamConsumer = ClientConfig::new()
//             .set("bootstrap.servers", BOOTSTRAP_SERVERS)
//             .set("group.id", "my_consumer_group")
//             .set("enable.auto.commit", "false")
//             .set("auto.offset.reset", "earliest")
//             .create()
//             .expect("Consumer creation error");

//         tokio::spawn(async move {
//             if let Err(e) = handle_connection(stream, producer, consumer).await {
//                 eprintln!("Error handling connection: {}", e);
//             }
//         });
//     }
// }

async fn handle_connection(stream: TcpStream) {
    let (mut reader, mut writer) = tokio::io::split(stream);

    // move the reader
    tokio::spawn(async move {
        let mut buf = vec![0; 1024];

        // In a loop, read data from the socket and write the data back.
        loop {
            let n = reader
                .read(&mut buf)
                .await
                .expect("failed to read data from socket");

            if n == 0 {
                return;
            }

            println!("{:?}", buf);
        }
    });

    // move the writer
    tokio::spawn(async move {
        let mut loop_count = 1_i32;


        // In a loop, read data from the socket and write the data back.
        loop {
            // let bytes = loop_count.to_ne_bytes();

            let bytes = b"Hello!";
            writer
                .write_all(bytes)
                .await
                .expect("failed to write data to socket");

            sleep(Duration::from_millis(1)).await;

            loop_count += 1;
        }
    });
}

// https://v0-1--tokio.netlify.app/docs/getting-started/echo/
// https://github.com/tokio-rs/tokio/blob/master/examples/echo.rs

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Allow passing an address to listen on as the first argument of this
    // program, but otherwise we'll just set up our TCP listener on
    // 127.0.0.1:8080 for connections.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6665".to_string());

    // Next up we create a TCP listener which will listen for incoming
    // connections. This TCP listener is bound to the address we determined
    // above and must be associated with an event loop.
    let listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);
    

    loop {
        // Asynchronously wait for an inbound socket.
        let (stream, _) = listener.accept().await?;

        // And this is where much of the magic of this server happens. We
        // crucially want all clients to make progress concurrently, rather than
        // blocking one on completion of another. To achieve this we use the
        // `tokio::spawn` function to execute the work in the background.
        //
        // Essentially here we're executing a new task to run concurrently,
        // which will allow all of our clients to be processed concurrently.
        tokio::spawn(async move {
            handle_connection(stream).await;
        });

        
    }
}