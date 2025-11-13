use env_logger::{Builder, Target};
use futures_util::{SinkExt, StreamExt};
use log::{LevelFilter, info, error};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn handle_client(stream: TcpStream) {
    let addr = stream.peer_addr().unwrap();
    info!("New connection: {}", addr);

    let ws = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(err) => {
            error!("Handshake failed: {}", err);
            return;
        }
    };

    let (mut write, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                info!("Received from {}: {}", addr, text);
                let _ = write.send(Message::Text(text)).await;
            }
            Ok(Message::Close(_)) => {
                info!("Client disconnected: {}", addr);
                break;
            }
            Err(e) => error!("Error from {}: {}", addr, e),
            _ => {}
        }
    }

    info!("Connection closed: {}", addr);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Builder::new().target(Target::Stdout).filter_level(LevelFilter::Info).init();

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    info!("WebSocket running on ws://127.0.0.1:8080");

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(handle_client(stream));
    }

    Ok(())
}
