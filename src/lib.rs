use std::future::Future;
use std::io;
use std::iter::Cycle;
use std::sync::Arc;
use std::thread;
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::mpsc::error::SendError;

type RtCompletionSender<O> = std::sync::mpsc::Sender<Result<Option<O>, Error>>;
type WorkerTaskSender<T> = tokio::sync::mpsc::UnboundedSender<T>;
type WorkerTaskReceiver<T> = tokio::sync::mpsc::UnboundedReceiver<T>;
type WorkerSenderRoundRobin<T> = Cycle<std::vec::IntoIter<WorkerTaskSender<T>>>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to obtain CPU core information")]
    FailedToObtainCpuCoreInfo,

    #[error("at least one core should be available for a worker, but system has only one core")]
    NoWorkerCores,

    #[error("failed to spawn tokio runtime: {0}")]
    FailedToSpawnTokioRuntime(io::Error),

    #[error("the main runtime's root task has panicked")]
    MainRootTaskPanicked,
}

pub struct TaskSender<T>(WorkerSenderRoundRobin<T>)
where
    T: Send + 'static;

impl<T> TaskSender<T>
where
    T: Send + 'static,
{
    pub fn send(&mut self, task: T) -> Result<(), SendError<T>> {
        self.0.next().unwrap().send(task)
    }
}

pub trait TransferAsyncIo {
    fn transfer_async_io(self) -> Self;
}

impl TransferAsyncIo for TcpStream {
    fn transfer_async_io(self) -> Self {
        TcpStream::from_std(TcpStream::into_std(self).unwrap()).unwrap()
    }
}

impl TransferAsyncIo for UnixStream {
    fn transfer_async_io(self) -> Self {
        UnixStream::from_std(UnixStream::into_std(self).unwrap()).unwrap()
    }
}

impl TransferAsyncIo for () {
    fn transfer_async_io(self) -> Self {
        ()
    }
}

pub fn block_on<T, M, W, O>(
    main: impl FnOnce(TaskSender<T>) -> M + Send + 'static,
    worker: impl Fn(T) -> W + Send + Sync + 'static,
) -> Result<O, Error>
where
    O: Send + 'static,
    T: Send + TransferAsyncIo + 'static,
    M: Future<Output = O> + 'static,
    W: Future<Output = ()> + 'static,
{
    let core_ids = core_affinity::get_core_ids().ok_or(Error::FailedToObtainCpuCoreInfo)?;

    if core_ids.len() < 2 {
        return Err(Error::NoWorkerCores);
    }

    let mut core_ids = core_ids.into_iter();
    let main_core_id = core_ids.next().expect("should have main core ID");
    let (rt_completion_tx, rt_completion_rx) = std::sync::mpsc::channel();
    let mut worker_task_txs = vec![];
    let worker = Arc::new(worker);

    for core_id in core_ids {
        let (task_tx, task_rx) = tokio::sync::mpsc::unbounded_channel();
        let rt_completion_tx = rt_completion_tx.clone();
        let worker = Arc::clone(&worker);

        worker_task_txs.push(task_tx);

        spawn_worker_runtime(core_id, rt_completion_tx, task_rx, worker);
    }

    let task_sender = TaskSender(worker_task_txs.into_iter().cycle());

    spawn_runtime_thread(main_core_id, rt_completion_tx, move || async move {
        Some(main(task_sender).await)
    });

    loop {
        if let Some(res) = rt_completion_rx
            .recv()
            .map_err(|_| Error::MainRootTaskPanicked)??
        {
            return Ok(res);
        }
    }
}

fn spawn_worker_runtime<T, W, O>(
    core_id: core_affinity::CoreId,
    rt_completion_tx: RtCompletionSender<O>,
    mut task_rx: WorkerTaskReceiver<T>,
    worker: Arc<dyn Fn(T) -> W + Send + Sync + 'static>,
) where
    O: Send + 'static,
    T: Send + TransferAsyncIo + 'static,
    W: Future<Output = ()> + 'static,
{
    spawn_runtime_thread(core_id, rt_completion_tx, move || async move {
        // NOTE: exhausting this channel means that main has been tore down, so
        // we gracefully terminate.
        while let Some(task) = task_rx.recv().await {
            tokio::task::spawn_local({
                let worker = Arc::clone(&worker);
                let task = task.transfer_async_io();

                async move { worker(task).await }
            });
        }

        None
    });
}

fn spawn_runtime_thread<O, F>(
    core_id: core_affinity::CoreId,
    rt_completion_tx: RtCompletionSender<O>,
    block_on: impl FnOnce() -> F + Send + 'static,
) where
    O: Send + 'static,
    F: Future<Output = Option<O>>,
{
    thread::spawn(move || {
        core_affinity::set_for_current(core_id);

        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(err) => {
                let _ = rt_completion_tx.send(Err(Error::FailedToSpawnTokioRuntime(err)));
                return;
            }
        };

        let res =
            rt.block_on(async move { tokio::task::LocalSet::new().run_until(block_on()).await });

        let _ = rt_completion_tx.send(Ok(res));
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::thread::ThreadId;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;
    use tokio::time::{sleep, Duration};

    #[test]
    fn panic_in_worker_doesnt_shutdown_rt() {
        let main = |mut sender: TaskSender<()>| async move {
            let _ = sender.send(());
            sleep(Duration::from_millis(50)).await;

            "success"
        };

        let worker = |_: ()| async move { panic!("oops") };
        let res = block_on(main, worker);

        assert!(matches!(res, Ok("success")));
    }

    #[test]
    fn panic_in_main() {
        let main = |_: TaskSender<()>| async move { panic!("oops") };
        let worker = |_: ()| async move {};

        let res = block_on(main, worker);

        assert!(matches!(res, Err(Error::MainRootTaskPanicked)));
    }

    #[tokio::test]
    async fn uds_server() {
        const MESSAGE: &[u8] = b"Hello world!";

        let threads_used = Arc::new(Mutex::new(HashMap::<ThreadId, usize>::new()));
        let addr = format!("/tmp/tokio-sn-test-{}.sock", nanoid::nanoid!());

        let main = {
            let addr = addr.clone();

            move |mut sender: TaskSender<UnixStream>| async move {
                let listener = UnixListener::bind(addr).unwrap();

                while let Ok((stream, _)) = listener.accept().await {
                    sender.send(stream).unwrap();
                }
            }
        };

        let worker = {
            let threads_used = Arc::clone(&threads_used);

            move |mut stream: UnixStream| {
                let threads_used = Arc::clone(&threads_used);

                async move {
                    threads_used
                        .lock()
                        .unwrap()
                        .entry(thread::current().id())
                        .and_modify(|c| *c += 1)
                        .or_default();

                    stream.write_all(MESSAGE).await.unwrap();
                    stream.flush().await.unwrap();
                }
            }
        };

        thread::spawn(move || block_on(main, worker).unwrap());

        // NOTE: wait for server to start accepting connections
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut conns = vec![];

        for _ in 0..1000 {
            conns.push({
                let addr = addr.clone();

                async move {
                    let mut stream = UnixStream::connect(addr).await.unwrap();
                    let mut buf = [0u8; MESSAGE.len()];

                    stream.read_exact(&mut buf).await.unwrap();

                    assert_eq!(buf, MESSAGE);
                }
            });
        }

        futures::future::join_all(conns).await;

        // NOTE: check that work was distributed across threads
        let threads_used = threads_used.lock().unwrap();

        assert_eq!(
            threads_used.len(),
            core_affinity::get_core_ids().unwrap().len() - 1
        );

        assert!(threads_used.values().all(|jobs| *jobs > 1));
    }
}
