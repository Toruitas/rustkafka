use tokio::time::{sleep, Duration};
use log::{info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use std::env;
use std::error::Error;
use tokio::net::{TcpListener, TcpStream};
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use tokio::sync::mpsc;
// use std::error::Error;
// use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use futures::{StreamExt, TryStreamExt};
use rdkafka::message::{BorrowedMessage, OwnedMessage, Message};
// use rdkafka::message::Message;
// use rdkafka::error::KafkaResult;
const BOOTSTRAP_SERVERS: &str = "localhost:29092";
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

async fn handle_connection(stream: TcpStream, producer:FutureProducer) {
    let (mut reader, mut writer) = tokio::io::split(stream);

    let topic = "some_topic_all_can_look_at";
    let group_id = "my_consumer_group";


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

            let produce_future = producer.send(
                FutureRecord::to(&topic)
                    .key("some key")
                    .payload(&buf),
                Duration::from_secs(0),
            );

            match produce_future.await {
                Ok(delivery) => println!("Sent: {:?}", delivery),
                Err((e, _)) => println!("Error: {:?}", e),
            }

            println!("Inserted into kafka {:?}", buf);
            // reset the buffer
            buf = vec![0; 1024];
        }
    });

    // move the writer
    tokio::spawn(async move {
        writer
            .write_all(b"hello")
            .await
            .expect("failed to write data to socket");

        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", BOOTSTRAP_SERVERS)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Consumer creation failed");


        consumer
            .subscribe(&[&topic])
            .expect("Can't subscribe to specified topic");

        println!("Subscribed to kafka topic");

        loop {
            match consumer.recv().await {
                Err(e) => warn!("Kafka error: {}", e),
                Ok(m) => {
                    let payload = match m.payload() {
                        None => continue,
                        Some(s) => s,
                    };

                    info!("key: '{:?}', payload: '{:?}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                            m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());

                    // if let Some(headers) = m.headers() {
                    //     for header in headers.iter() {
                    //         info!("  Header {:#?}: {:?}", header.key, header.value);
                    //     }
                    // }
                    // consumer.commit_message(&m, CommitMode::Async).unwrap();

                    writer
                        .write_all(payload)
                        .await
                        .expect("failed to write data to socket");

                }
            };
        }

        // loop {
        //     for ms in consumer.iter(){
        //         let msg = ms.unwrap();
        //         // let key: &str = msg.key_view().unwrap().unwrap(); //lol
        //         let value = msg.payload().unwrap();
        //         println!("{:?}", str::from_utf8(value).unwrap());
        //     }
        // }

        // let stream_processor = consumer
        //     .stream()
        //     .try_for_each(|borrowed_message| 
        //     {
        //     async move {
                
        //         let owned_message = borrowed_message.detach();
        //         let value = owned_message.payload().unwrap();

        //         info!("key: '{:?}', payload: '{:?}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
        //         owned_message.key(), value, owned_message.topic(), owned_message.partition(), owned_message.offset(), owned_message.timestamp());
 
        //         // useful when turning into json, but not here
        //         // let payload = match owned_message.payload_view::<str>() {
        //         //     None => "",
        //         //     Some(Ok(s)) => s,
        //         //     Some(Err(e)) => {
        //         //         warn!("Error while deserializing message payload: {:?}", e);
        //         //         ""
        //         //     }
        //         // };

                // writer
                //     .write_all(value)
                //     .await
                //     .expect("failed to write data to socket");
        //         Ok(())
        //     }
        // });


        // // In a loop, read data from the socket and write the data back.
        // loop {
            // let bytes = 1_i32.to_ne_bytes();
        let bytes = "hello".bytes();
        let e = b"hello";

            // let bytes = b"Hello!";
            // writer
            //     .write_all(bytes)
            //     .await
            //     .expect("failed to write data to socket");

        //     sleep(Duration::from_millis(1)).await;
        // }
    });
}

// https://v0-1--tokio.netlify.app/docs/getting-started/echo/
// https://github.com/tokio-rs/tokio/blob/master/examples/echo.rs

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Allow passing an address to listen on as the first argument of this
    // program, but otherwise we'll just set up our TCP listener on
    // 127.0.0.1:6665 for connections.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6665".to_string());

    // Next up we create a TCP listener which will listen for incoming
    // connections. This TCP listener is bound to the address we determined
    // above and must be associated with an event loop.
    let listener = TcpListener::bind(&addr).await?;

    println!("Listening on: {}", addr);


    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP_SERVERS)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

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
        let producer = producer.clone();
        tokio::spawn(async move {
            handle_connection(stream, producer).await;
        });

        
    }
}