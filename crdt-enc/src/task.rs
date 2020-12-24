use anyhow::Result;
use futures::{
    channel::mpsc,
    future::{self, BoxFuture, Future, FutureExt},
    stream::FuturesUnordered,
    stream::{FusedStream, StreamExt},
    task::{self, Poll, SpawnError},
};
use std::{fmt, pin::Pin, result::Result as StdResult};

// thread_local! {
//     // need to use `Box<Any>` here, <https://github.com/rust-lang/rust/issues/57775>
//     static TL_DATA: RefCell<Option<Box<dyn Any + 'static>>> = RefCell::new(None);
// }

// pub struct TaskMgrAccessor;

// impl TaskMgrAccessor {
//     pub fn with<T, F, R>(f: F) -> R
//     where
//         T: 'static,
//         F: FnOnce(&mut T) -> R,
//     {
//         TL_DATA.with(|data| {
//             let mut data = data.borrow_mut();
//             let data = data.as_mut().expect("TaskMgrAccessor data not set");
//             if let Some(data) = data.downcast_mut::<T>() {
//                 f(data)
//             } else {
//                 panic!(format!(
//                     "Data in TaskMgrAccessor has wrong type, expected type: {}",
//                     any::type_name::<T>()
//                 ));
//             }
//         })
//     }

//     pub fn set_with<T, F, R>(val: T, f: F) -> (T, R)
//     where
//         T: 'static,
//         F: FnOnce() -> R,
//     {
//         TL_DATA.with(|data| {
//             let mut data = data.borrow_mut();
//             *data = Some(Box::new(val));
//         });

//         let res = f();

//         let val = TL_DATA.with(|data| {
//             let mut data = data.borrow_mut();
//             let data = data.take().expect("TaskMgrAccessor data not set");
//             if let Ok(data) = data.downcast::<T>() {
//                 *data
//             } else {
//                 panic!(format!(
//                     "Data in TaskMgrAccessor has wrong type, expected type: {}",
//                     any::type_name::<T>()
//                 ));
//             }
//         });

//         (val, res)
//     }
// }

pub struct TaskMgrExecutor {
    futs: FuturesUnordered<BoxFuture<'static, Result<()>>>,
    rx: mpsc::UnboundedReceiver<BoxFuture<'static, Result<()>>>,
}

impl Future for TaskMgrExecutor {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        while let Poll::Ready(Some(fut)) = self.rx.poll_next_unpin(ctx) {
            self.futs.push(fut);
        }

        if self.futs.is_empty() {
            if self.rx.is_terminated() {
                // no running tasks & the receiver closed => exit
                return Poll::Ready(Ok(()));
            } else {
                return Poll::Pending;
            }
        }

        while let Poll::Ready(res) = self.futs.poll_next_unpin(ctx) {
            match res {
                Some(Ok(())) => {}
                Some(Err(err)) => {
                    return Poll::Ready(Err(err));
                }
                None => {
                    return Poll::Ready(Ok(()));
                }
            }
        }

        Poll::Pending
    }
}

impl fmt::Debug for TaskMgrExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskMgrExecutor").finish()
    }
}

#[derive(Clone)]
pub struct TaskMgr {
    tx: mpsc::UnboundedSender<BoxFuture<'static, Result<()>>>,
}

impl TaskMgr {
    pub fn new() -> (Self, TaskMgrExecutor) {
        let (tx, rx) = mpsc::unbounded();

        (
            TaskMgr { tx },
            TaskMgrExecutor {
                futs: FuturesUnordered::new(),
                rx,
            },
        )
    }

    pub fn spawn<F>(&self, fut: F) -> StdResult<(), SpawnError>
    where
        F: 'static + Send + Future<Output = Result<()>>,
    {
        self.tx
            .unbounded_send(fut.boxed())
            .map_err(|_| SpawnError::shutdown())?;
        Ok(())
    }

    pub fn spawn_with_handle<F>(
        &self,
        fut: F,
    ) -> StdResult<future::RemoteHandle<F::Output>, SpawnError>
    where
        F: 'static + Send + Future,
        F::Output: 'static + Send,
    {
        let (remote, handle) = fut.remote_handle();
        self.tx
            .unbounded_send(remote.map(|()| Result::Ok(())).boxed())
            .map_err(|_| SpawnError::shutdown())?;
        Ok(handle)
    }
}

impl fmt::Debug for TaskMgr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskMgr").finish()
    }
}
