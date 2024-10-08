import sys
import traceback
from threading import Thread
from time import sleep


def run_in_parallel(function, arg_list, thread_count=10, update_callback=None, **kwargs):
    result_list = [None] * len(arg_list)
    threads = []
    for parallel_index in range(thread_count):
        thread = Thread(target=_run, args=(function, arg_list, result_list, parallel_index, thread_count, update_callback, kwargs))
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    return result_list


def _run(function, arg_list, result_list, parallel_index, parallel_count, update_callback, kwargs):
    for i in range(parallel_index, len(arg_list), parallel_count):
        try:
            result = function(*arg_list[i])
            result_list[i] = result
        except Exception as e:
            print(f"Encountered exception while parallel processing #{i}/{len(arg_list)}: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

        if update_callback:
            update_callback(**kwargs)


if __name__ == "__main__":
    def plus_one(original):
        sleep(0.01)
        return original + 1


    results = run_in_parallel(plus_one, [[a] for a in range(100)], thread_count=32)
    print(results)
