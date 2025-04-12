use anyhow::anyhow;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::any;
use axum::Router;
use common::{
    encode_msgpack_value, WorkflowRunTask, WorkflowRunTaskId, WorkflowRunTaskStatus, WsClientMessage,
    WsServerMessage,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::select;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = Router::new().route("/api/worker", any(worker_handler));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Deserialize, Default, PartialEq)]
enum FormatType {
    #[serde(rename = "json")]
    JSON,
    #[default]
    #[serde(rename = "msgpack")]
    MsgPack,
}

#[derive(Deserialize, Default)]
struct Format {
    f: Option<FormatType>,
}

async fn worker_handler(ws: WebSocketUpgrade, query: Query<Format>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, query.0.f.unwrap_or_default()))
}

async fn handle_socket(socket: WebSocket, format_type: FormatType) {
    if let Err(error) = handle_socket_inner(socket, format_type).await {
        println!("{error}")
    }
}

async fn handle_socket_inner(mut socket: WebSocket, format_type: FormatType) -> anyhow::Result<()> {
    let message = match socket.recv().await {
        Some(inner) => inner?,
        None => return Err(anyhow!("WebSocket closed by client")),
    };
    let startup_message = match message {
        Message::Binary(bytes) => rmp_serde::decode::from_slice(&bytes)?,
        Message::Text(text) => serde_json::from_str(text.as_str())?,
        Message::Ping(_) | Message::Pong(_) => return Ok(()),
        Message::Close(close) => {
            println!("Received close message: {close:?}");
            return Ok(());
        },
    };
    let WsClientMessage::Start(task_ids) = startup_message else {
        println!("Received non-startup message as initial message. {startup_message:?}");
        return Ok(());
    };
    println!("Starting web socket handler for tasks: {task_ids:?}");

    let (mut sender, mut receiver) = socket.split();
    loop {
        select! {
            msg = receiver.next() => {
                let message = match msg {
                    Some(inner) => inner?,
                    None => return Err(anyhow!("WebSocket closed by client")),
                };
                let client_message: WsClientMessage = match message {
                    Message::Binary(bytes) => rmp_serde::decode::from_slice(&bytes)?,
                    Message::Text(text) => serde_json::from_str(text.as_str())?,
                    Message::Ping(_) | Message::Pong(_) => continue,
                    Message::Close(close) => {
                        println!("Received close message: {close:?}");
                        break;
                    }
                };
                match client_message {
                    WsClientMessage::Start(_) => continue,
                    WsClientMessage::Ready { task_id, task_slots } => {
                        if !task_ids.contains(&task_id) {
                            continue;
                        }
                        let message = WsServerMessage::Run(WorkflowRunTask {
                            workflow_run_task_id: WorkflowRunTaskId {
                                run_id: Default::default(),
                                task_order: 1,
                            },
                            task_id: Default::default(),
                            input_data: None,
                            output_data: None,
                            task_status: WorkflowRunTaskStatus::Pending,
                        });
                        let ws_message = match format_type {
                            FormatType::JSON => Message::text(serde_json::to_string(&message)?),
                            FormatType::MsgPack => Message::binary(encode_msgpack_value(&message)?),
                        };
                        sender.send(ws_message).await?;
                    }
                    WsClientMessage::Accept(_) => {}
                    WsClientMessage::Reject(_) => {}
                    WsClientMessage::Update { workflow_run_task_id, progress } => {
                        println!("{workflow_run_task_id:?}: {progress}%")
                    }
                    WsClientMessage::Result(_) => {}
                    WsClientMessage::Close => break,
                }
            }
        }
    }

    Ok(())
}
