use common::{
    TaskId, TaskProgressUpdate, TaskResult, WorkerId, WorkflowRunTask, WsClientMessage,
    WsServerMessage,
};
use crossbeam::channel::{Receiver, Sender, unbounded};
use crossbeam::select;
use serde::Serialize;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::Arc;
use std::thread::{JoinHandle, spawn};
use std::time::Duration;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Bytes, Message, WebSocket, connect};
use uuid::Uuid;

const CLEAR_TASK_TIMEOUT: Duration = Duration::from_secs(1);

pub struct Worker {
    worker_id: WorkerId,
    runners: HashMap<TaskId, WorkflowTaskRunnerConfig>,
}

impl Worker {
    pub fn run(self) {
        let (task_queue_sender, task_queue_receiver) = unbounded();
        let (ws_write_sender, ws_write_receiver) = unbounded();
        let running_worker = RunningWorker::new(self.worker_id, ws_write_sender.clone());
        let url = match std::env::var("BERTHA_URL") {
            Ok(inner) => inner,
            Err(error) => {
                println!("{error}");
                return;
            },
        };

        let runners: HashMap<TaskId, Sender<(RunningWorker, WorkflowRunTask)>> = self
            .runners
            .into_iter()
            .map(|(task_id, config)| {
                let (sender, executor) =
                    WorkflowTaskExecutor::create(config, ws_write_sender.clone());
                executor.start();
                (task_id, sender)
            })
            .collect();
        let task_ids: Vec<TaskId> = runners.keys().cloned().collect();

        let (mut web_socket, _) = connect(&url).expect("");

        let start = WsClientMessage::Start(task_ids);
        let bytes = match encode_value(&start) {
            Ok(inner) => inner,
            Err(error) => {
                println!("{error}");
                return;
            },
        };
        if let Err(error) = web_socket.send(Message::Binary(bytes)) {
            println!("Could not send initial client packet. {error}");
            return;
        };

        let ws_operation_handle =
            spawn(move || process_ws_operations(web_socket, task_queue_sender, ws_write_receiver));

        loop {
            let workflow_run_task = match task_queue_receiver.recv() {
                Ok(inner) => inner,
                Err(error) => {
                    println!("{error}");
                    break;
                },
            };

            let Some(task_queue) = runners.get(workflow_run_task.task_id()) else {
                println!("Received task with no corresponding task_id. {workflow_run_task:?}");
                continue;
            };

            if let Err(error) = task_queue.send((running_worker.clone(), workflow_run_task)) {
                println!("{error}");
            };
        }

        running_worker.send_message(WsClientMessage::Close);
        if let Err(error) = ws_operation_handle.join() {
            println!("{error:?}");
        }
    }
}

fn process_ws_operations(
    mut web_socket: WebSocket<MaybeTlsStream<TcpStream>>,
    task_queue_sender: Sender<WorkflowRunTask>,
    ws_write_receiver: Receiver<WsClientMessage>,
) {
    loop {
        if web_socket.can_read() {
            let bytes = match web_socket.read() {
                Ok(Message::Binary(inner)) => inner,
                Ok(Message::Close(_)) => break,
                Ok(Message::Ping(_)) => continue,
                Ok(message) => {
                    println!("Unexpected message: {message}");
                    continue;
                },
                Err(error) => {
                    println!("{error}");
                    return;
                },
            };
            let ws_message: WsServerMessage = match rmp_serde::from_slice(&bytes) {
                Ok(inner) => inner,
                Err(error) => {
                    println!("{error}");
                    continue;
                },
            };
            match ws_message {
                WsServerMessage::Run(workflow_run_task) => {
                    if let Err(error) = task_queue_sender.send(workflow_run_task) {
                        println!("{error}")
                    }
                },
                WsServerMessage::Close => return,
            }
            continue;
        }

        let Ok(message) = ws_write_receiver.try_recv() else {
            continue;
        };
        if let WsClientMessage::Close = message {
            break;
        }

        let bytes = match encode_value(&message) {
            Ok(inner) => inner,
            Err(error) => {
                println!("{error}");
                break;
            },
        };
        if let Err(error) = web_socket.send(Message::Binary(bytes)) {
            println!("Could not send closing client packet. {error}");
            return;
        };
    }

    let bytes = match encode_value(&WsClientMessage::Close) {
        Ok(inner) => inner,
        Err(error) => {
            println!("{error}");
            return;
        },
    };
    if let Err(error) = web_socket.send(Message::Binary(bytes)) {
        println!("Could not send closing client packet. {error}");
    };
}

#[derive(Default)]
pub struct WorkerBuilder {
    executors: HashMap<TaskId, WorkflowTaskRunnerConfig>,
}

impl WorkerBuilder {
    pub fn add_runner(self, task_id: TaskId, runner: WorkflowTaskRunner) -> Self {
        self.add_runner_with_config(
            task_id,
            WorkflowTaskRunnerConfig {
                runner,
                task_thread_count: 1,
            },
        )
    }

    pub fn add_runner_with_config(
        mut self,
        task_id: TaskId,
        workflow_task_runner_config: WorkflowTaskRunnerConfig,
    ) -> Self {
        self.executors.insert(task_id, workflow_task_runner_config);
        self
    }

    pub fn build(self) -> Worker {
        Worker {
            worker_id: WorkerId::from(Uuid::now_v7()),
            runners: self.executors,
        }
    }
}

pub type WorkflowTaskRunner = fn(RunningWorker, WorkflowRunTask) -> TaskResult;

pub struct WorkflowTaskRunnerConfig {
    runner: WorkflowTaskRunner,
    task_thread_count: usize,
}

pub struct WorkflowTaskExecutor {
    config: WorkflowTaskRunnerConfig,
    task_receiver: Receiver<(RunningWorker, WorkflowRunTask)>,
    ws_write_sender: Sender<WsClientMessage>,
    running_tasks: Vec<JoinHandle<TaskResult>>,
}

impl WorkflowTaskExecutor {
    fn create(
        config: WorkflowTaskRunnerConfig,
        ws_write_sender: Sender<WsClientMessage>,
    ) -> (Sender<(RunningWorker, WorkflowRunTask)>, Self) {
        let (sender, receiver) = unbounded();
        let running_tasks = Vec::with_capacity(config.task_thread_count);
        let executor = Self {
            config,
            task_receiver: receiver,
            ws_write_sender,
            running_tasks,
        };
        (sender, executor)
    }

    fn start(self) {
        spawn(move || self.run());
    }

    fn run(mut self) {
        loop {
            select! {
                recv(self.task_receiver) -> task => {
                    let (running_worker, task) = match task {
                        Ok(inner) => inner,
                        Err(error) => {
                            println!("{error}");
                            break;
                        }
                    };

                    if self.running_tasks.len() >= self.config.task_thread_count {
                        let message = WsClientMessage::Reject(TaskResult::reject(task));
                        if let Err(error) = self.ws_write_sender.send(message) {
                            println!("{error}");
                            break;
                        }
                        continue;
                    }

                    let message = WsClientMessage::Accept(TaskResult::accept(&task));
                    let worker = self.config.runner;
                    self.running_tasks.push(spawn(move || worker(running_worker, task)));
                    if let Err(error) = self.ws_write_sender.send(message) {
                        println!("{error}");
                        break;
                    }
                }
                default(CLEAR_TASK_TIMEOUT) => {
                    self.remove_completed_tasks();
                    let message = WsClientMessage::Ready(self.config.task_thread_count - self.running_tasks.len());
                    if let Err(error) = self.ws_write_sender.send(message) {
                        println!("{error}");
                        break;
                    }
                }
            }
        }
    }

    fn remove_completed_tasks(&mut self) {
        let completed_tasks: Vec<usize> = self
            .running_tasks
            .iter()
            .enumerate()
            .filter_map(|(i, handle)| if handle.is_finished() { Some(i) } else { None })
            .collect();
        for i in completed_tasks {
            let task_result = match self.running_tasks.remove(i).join() {
                Ok(inner) => inner,
                Err(error) => {
                    println!("{error:?}");
                    continue;
                },
            };

            let message = WsClientMessage::Result(task_result);
            if let Err(error) = self.ws_write_sender.send(message) {
                println!("{error}")
            }
        }
    }
}

fn encode_value<T: Serialize>(value: &T) -> Result<Bytes, rmp_serde::encode::Error> {
    let bytes = rmp_serde::encode::to_vec(&value)?;
    Ok(Bytes::from(bytes))
}

pub struct RunningWorkerInner {
    worker_id: WorkerId,
    update_sender: Sender<WsClientMessage>,
}

pub struct RunningWorker(Arc<RunningWorkerInner>);

impl Clone for RunningWorker {
    fn clone(&self) -> Self {
        RunningWorker(self.0.clone())
    }
}

impl RunningWorker {
    fn new(worker_id: WorkerId, update_sender: Sender<WsClientMessage>) -> Self {
        Self(Arc::new(RunningWorkerInner {
            worker_id,
            update_sender,
        }))
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.0.worker_id
    }

    fn send_message(&self, message: WsClientMessage) {
        if let Err(error) = self.0.update_sender.send(message) {
            println!("Error sending task update. {error}");
        }
    }

    pub fn send_task_progress_update(&self, workflow_run_task: &WorkflowRunTask, progress: u8) {
        let update = TaskProgressUpdate::new(workflow_run_task, progress);
        let message = WsClientMessage::Update(update);
        self.send_message(message)
    }
}
