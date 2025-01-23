import concurrent.futures
import threading
import time
import signal
import sys

executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
stop_event = threading.Event()  # 创建一个全局的 Event，异步通知其他线程


# 模拟耗时的任务
def task(n):
    global stop_event
    print(f"Task {n} started.")
    for i in range(60):
        if stop_event.is_set():
            print(f"Task {n} stopped.")
            break
        print(f"Task {n}: {i}")
        time.sleep(1)  # 模拟任务耗时
    print(f"Task {n} finished.")
    return f"Result from task {n}"


# 处理 Ctrl+C 信号
def signal_handler(sig, frame):
    global stop_event
    print("Ctrl+C pressed. Shutting down the executor ungracefully.")
    stop_event.set()
    executor.shutdown(wait=True, cancel_futures=True)  # 等待所有任务完成后关闭线程池
    print("sys exit(0)")
    sys.exit(0)  # 程序退出


def main():
    # 设置 Ctrl+C 的信号处理
    signal.signal(signal.SIGINT, signal_handler)

    # 创建线程池，最多允许 3 个线程同时执行任务
    with executor:
        futures = [executor.submit(task, i) for i in range(5)]  # 提交 5 个任务

        # 等待任务完成
        for future in concurrent.futures.as_completed(futures):
            print(future.result())  # 获取每个任务的结果


if __name__ == "__main__":
    main()
