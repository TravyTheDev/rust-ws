use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::HeaderValue,
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::{self, Sender, error::RecvError};
use tower_http::cors::{Any, CorsLayer, AllowOrigin};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionUpdate {
    user_id: String,
    x: f32,
    y: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum BroadcastEvent {
    Update {
        #[serde(rename = "userId")]
        user_id: String,
        x: f32,
        y: f32,
    },
    Leave {
        #[serde(rename = "userId")]
        user_id: String,
    },
}

#[tokio::main]
async fn main() {
    let (tx, _) = broadcast::channel::<BroadcastEvent>(100);
    let app = app(tx);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:PORT").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

fn app(tx: Sender<BroadcastEvent>) -> Router {
    let cors_layer = CorsLayer::new()
        .allow_methods(Any)
        .allow_origin(AllowOrigin::list([
            "https://MY_SITE".parse::<HeaderValue>().unwrap(),
            "https://www.MY_SITE".parse::<HeaderValue>().unwrap(),
        ]));

    Router::new()
        .route("/", get(|| async { "Home" }))
        .route("/smoke", get(chat_handler))
        .with_state(tx)
        .layer(cors_layer)
}

async fn chat_handler(
    State(tx): State<Sender<BroadcastEvent>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(|websocket: WebSocket| handle_websocket(tx, websocket))
}

async fn handle_websocket(tx: Sender<BroadcastEvent>, websocket: WebSocket) {
    let (mut sender, mut receiver) = websocket.split();
    let mut rx = tx.subscribe();

    let send_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if let Ok(json) = serde_json::to_string(&event) {
                        if sender.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                }
                Err(RecvError::Lagged(skipped)) => {
                    eprintln!("Client lagged behind by {skipped} messages");
                }
                Err(RecvError::Closed) => break,
            }
        }
    });

    let mut connected_user_id: Option<String> = None;

    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Text(content) => match serde_json::from_str::<PositionUpdate>(&content) {
                Ok(parsed) => {
                    connected_user_id = Some(parsed.user_id.clone());
                    let _ = tx.send(BroadcastEvent::Update {
                        user_id: parsed.user_id,
                        x: parsed.x,
                        y: parsed.y,
                    });
                }
                Err(e) => {
                    eprintln!("Invalid PositionUpdate payload: {e}");
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    if let Some(user_id) = connected_user_id {
        let _ = tx.send(BroadcastEvent::Leave { user_id });
    }
    send_task.abort();
}
