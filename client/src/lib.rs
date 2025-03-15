use common::{TaskId, TaskProgressUpdate, TaskResult, WorkerId, WorkflowRunTask, WsMessage};
use futures_util::future::BoxFuture;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tokio_tungstenite::tungstenite::{Bytes, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use uuid::Uuid;

const CLEAR_TASK_TIMEOUT: Duration = Duration::from_secs(1);

enum InternalTaskProgressUpdate {
    Update(TaskProgressUpdate),
    Result(TaskResult),
    Close,
}

pub type WorkflowTaskExecutor =
    Box<dyn Fn(RunningWorker, WorkflowRunTask) -> BoxFuture<'static, TaskResult>>;

pub struct Worker {
    worker_id: WorkerId,
    executors: HashMap<TaskId, WorkflowTaskExecutor>,
}

impl Worker {
    pub async fn run(self) {
        let mut running_tasks: Vec<JoinHandle<TaskResult>> = vec![];
        let (update_sender, update_receiver) = tokio::sync::mpsc::unbounded_channel();
        let running_worker = RunningWorker::new(self.worker_id, update_sender);
        let url = match std::env::var("BERTHA_URL") {
            Ok(inner) => inner,
            Err(error) => {
                println!("{error}");
                return;
            },
        };
        let task_ids: Vec<TaskId> = self.executors.keys().cloned().collect();

        let (ws_stream, _) = connect_async(&url).await.expect("");
        let (mut ws_write, mut ws_read) = ws_stream.split();

        let start = WsMessage::Start(task_ids);
        let bytes = match encode_value(&start) {
            Ok(inner) => inner,
            Err(error) => {
                println!("{error}");
                return;
            },
        };
        if let Err(error) = ws_write.send(Message::Binary(bytes)).await {
            println!("Could not send initial client packet. {error}");
            return;
        };

        let task_progress_update_processor =
            tokio::spawn(process_task_progress_updates(ws_write, update_receiver));

        loop {
            let result = select! {
                biased;
                Some(result) = ws_read.next() => result,
                _ = sleep(CLEAR_TASK_TIMEOUT) => {
                    Self::clear_finished_tasks(running_worker.clone(), &mut running_tasks).await;
                    continue;
                }
            };
            let message = match result {
                Ok(Message::Binary(inner)) => inner,
                Ok(message) => {
                    println!("Unexpected message. {message}");
                    continue;
                },
                Err(error) => {
                    println!("{error}");
                    break;
                },
            };
            let ws_message: WsMessage = match rmp_serde::from_slice(&message) {
                Ok(inner) => inner,
                Err(error) => {
                    println!("{error}");
                    continue;
                },
            };

            let workflow_run_task = match ws_message {
                WsMessage::Run(inner) => inner,
                WsMessage::Close => break,
                WsMessage::Start(_) | WsMessage::Update(_) | WsMessage::Result(_) => {
                    println!("Unexpected message. {ws_message:?}");
                    continue;
                },
            };

            let Some(executor) = self.executors.get(workflow_run_task.task_id()) else {
                println!("Received task with no corresponding task_id. {workflow_run_task:?}");
                continue;
            };

            let task_handle = tokio::spawn(executor(running_worker.clone(), workflow_run_task));
            running_tasks.push(task_handle);
        }

        running_worker.send_message(InternalTaskProgressUpdate::Close);
        let mut ws_write = match task_progress_update_processor.await {
            Ok(inner) => inner,
            Err(error) => {
                println!("{error}");
                return;
            },
        };

        let bytes = match encode_value(&WsMessage::Close) {
            Ok(inner) => inner,
            Err(error) => {
                println!("{error}");
                return;
            },
        };
        if let Err(error) = ws_write.send(Message::Binary(bytes)).await {
            println!("Could not send closing client packet. {error}");
        };
    }

    async fn clear_finished_tasks(
        running_worker: RunningWorker,
        running_tasks: &mut Vec<JoinHandle<TaskResult>>,
    ) {
        if running_tasks.is_empty() {
            return;
        }
        let temp: Vec<usize> = running_tasks
            .iter()
            .enumerate()
            .filter_map(|(i, handle)| if handle.is_finished() { Some(i) } else { None })
            .collect();
        for index in temp {
            let task_result = match running_tasks.remove(index).await {
                Ok(inner) => inner,
                Err(error) => {
                    println!("{error}");
                    continue;
                },
            };
            running_worker.send_message(InternalTaskProgressUpdate::Result(task_result));
        }
    }
}

#[derive(Default)]
pub struct WorkerBuilder {
    executors: HashMap<TaskId, WorkflowTaskExecutor>,
}

impl WorkerBuilder {
    pub fn add_executor<R: Future<Output = TaskResult> + Send + Sync + 'static>(
        mut self,
        task_id: TaskId,
        executor: fn(RunningWorker, WorkflowRunTask) -> R,
    ) -> Self {
        self.executors.insert(
            task_id,
            Box::new(move |worker, workflow_run_task| {
                Box::pin(executor(worker, workflow_run_task))
            }),
        );
        self
    }

    pub fn build(self) -> Worker {
        Worker {
            worker_id: WorkerId::from(Uuid::now_v7()),
            executors: self.executors,
        }
    }
}

async fn process_task_progress_updates(
    mut ws_writer: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    mut receiver: UnboundedReceiver<InternalTaskProgressUpdate>,
) -> SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message> {
    while let Some(update) = receiver.recv().await {
        let message = match update {
            InternalTaskProgressUpdate::Update(progress) => WsMessage::Update(progress),
            InternalTaskProgressUpdate::Result(task_result) => WsMessage::Result(task_result),
            InternalTaskProgressUpdate::Close => break,
        };
        let bytes = match encode_value(&message) {
            Ok(inner) => inner,
            Err(error) => {
                println!("{error}");
                continue;
            },
        };
        if let Err(error) = ws_writer.send(Message::Binary(bytes)).await {
            println!("Failed to write progress update to websocket. {error}")
        }
    }
    ws_writer
}

fn encode_value<T: Serialize>(value: &T) -> Result<Bytes, rmp_serde::encode::Error> {
    let bytes = rmp_serde::encode::to_vec(&value)?;
    Ok(Bytes::from(bytes))
}

pub struct RunningWorkerInner {
    worker_id: WorkerId,
    update_sender: UnboundedSender<InternalTaskProgressUpdate>,
}

pub struct RunningWorker(Arc<RunningWorkerInner>);

impl Clone for RunningWorker {
    fn clone(&self) -> Self {
        RunningWorker(self.0.clone())
    }
}

impl RunningWorker {
    fn new(
        worker_id: WorkerId,
        update_sender: UnboundedSender<InternalTaskProgressUpdate>,
    ) -> Self {
        Self(Arc::new(RunningWorkerInner {
            worker_id,
            update_sender,
        }))
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.0.worker_id
    }

    fn send_message(&self, message: InternalTaskProgressUpdate) {
        if let Err(error) = self.0.update_sender.send(message) {
            println!("Error sending task update. {error}");
        }
    }

    pub fn send_task_progress_update(&self, workflow_run_task: &WorkflowRunTask, progress: u8) {
        let update = TaskProgressUpdate::new(workflow_run_task, progress);
        let message = InternalTaskProgressUpdate::Update(update);
        self.send_message(message)
    }
}
