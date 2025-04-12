use common::{
    TaskId, TaskResult, WorkerId, WorkflowRunTask, WsClientMessage, WsServerMessage,
    encode_msgpack_value,
};
use crossbeam::{
    channel::{Receiver, Sender, TryRecvError, unbounded},
    select,
};
use std::{
    collections::HashMap,
    net::TcpStream,
    sync::Arc,
    thread::{JoinHandle, spawn},
    time::Duration,
};
use tungstenite::{Message, WebSocket, connect, stream::MaybeTlsStream};
use uuid::Uuid;

const CLEAR_TASK_TIMEOUT: Duration = Duration::from_secs(1);

pub struct Worker {
    worker_id: WorkerId,
    runners: Vec<WorkflowTaskRunnerConfig>,
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
            .map(|config| {
                let task_id = config.task_id.clone();
                let (sender, executor) =
                    WorkflowTaskExecutor::create(config, ws_write_sender.clone());
                executor.start();
                (task_id, sender)
            })
            .collect();
        let task_ids: Vec<TaskId> = runners.keys().cloned().collect();

        let (mut web_socket, _) = connect(&url).expect("");

        let start = WsClientMessage::Start(task_ids);
        let bytes = match encode_msgpack_value(&start) {
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

        let message = match ws_write_receiver.try_recv() {
            Ok(inner) => inner,
            Err(TryRecvError::Empty) => continue,
            Err(TryRecvError::Disconnected) => break,
        };
        if let WsClientMessage::Close = message {
            break;
        }

        let bytes = match encode_msgpack_value(&message) {
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

    let bytes = match encode_msgpack_value(&WsClientMessage::Close) {
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
    executors: Vec<WorkflowTaskRunnerConfig>,
}

impl WorkerBuilder {
    /// Add a new [WorkflowTaskRunner] with default configuration. See [WorkflowTaskRunnerConfig]
    /// for more details of what the default attributes of a runner is
    pub fn add_runner(self, task_id: TaskId, runner: WorkflowTaskRunner) -> Self {
        self.add_runner_with_config(WorkflowTaskRunnerConfig {
            task_id,
            runner,
            task_thread_count: 1,
        })
    }

    /// Add a new [WorkflowTaskRunner] by supplying a full [WorkflowTaskRunnerConfig]
    pub fn add_runner_with_config(
        mut self,
        workflow_task_runner_config: WorkflowTaskRunnerConfig,
    ) -> Self {
        self.executors.push(workflow_task_runner_config);
        self
    }

    /// Terminate a [WorkerBuilder] and return a new [Worker] with the current state of the builder
    pub fn build(self) -> Worker {
        Worker {
            worker_id: WorkerId::from(Uuid::now_v7()),
            runners: self.executors,
        }
    }
}

/// Type alias for a function point that handles incoming task run requests, returning a
/// [TaskResult] to describe how the run went. Note this does not use the [Result] type since the
/// function definer should pack the error details into the result.
pub type WorkflowTaskRunner = fn(RunningWorker, WorkflowRunTask) -> TaskResult;

/// Client configuration for a [WorkflowTaskRunner]. Contains the runner and it's related
/// properties. To each the creation of these configs, there is a builder named
/// [WorkflowTaskRunnerConfigBuilder] that contains default values for some fields.
///
/// - `task_thread_count`, default = 1
pub struct WorkflowTaskRunnerConfig {
    task_id: TaskId,
    runner: WorkflowTaskRunner,
    /// Number of threads that this worker can spawn to handle task run requests. This should be
    /// proportional to the number of concurrent requests the worker should receive for this task
    /// and once that limit is reached, new requests sent to this worker will get a response of
    /// [WsClientMessage::Reject]
    task_thread_count: usize,
}

pub struct WorkflowTaskRunnerConfigBuilder {
    task_id: TaskId,
    runner: WorkflowTaskRunner,
    task_thread_count: usize,
}

impl WorkflowTaskRunnerConfigBuilder {
    pub fn new(task_id: TaskId, runner: WorkflowTaskRunner) -> WorkflowTaskRunnerConfigBuilder {
        Self {
            task_id,
            runner,
            task_thread_count: 1,
        }
    }

    pub fn task_thread_count(&mut self, task_thread_count: usize) -> &mut Self {
        self.task_thread_count = task_thread_count;
        self
    }
    
    pub fn build(self) -> WorkflowTaskRunnerConfig {
        WorkflowTaskRunnerConfig {
            task_id: self.task_id,
            runner: self.runner,
            task_thread_count: self.task_thread_count,
        }
    }
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
                        let message = WsClientMessage::reject(task);
                        if let Err(error) = self.ws_write_sender.send(message) {
                            println!("{error}");
                            break;
                        }
                        continue;
                    }

                    let message = WsClientMessage::accept(&task);
                    let worker = self.config.runner;
                    self.running_tasks.push(spawn(move || worker(running_worker, task)));
                    if let Err(error) = self.ws_write_sender.send(message) {
                        println!("{error}");
                        break;
                    }
                }
                default(CLEAR_TASK_TIMEOUT) => {
                    self.remove_completed_tasks();
                    let message = WsClientMessage::Ready {
                        task_id: self.config.task_id.clone(),
                        task_slots: self.config.task_thread_count - self.running_tasks.len()
                    };
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
        let message = WsClientMessage::update(workflow_run_task, progress);
        self.send_message(message)
    }
}
