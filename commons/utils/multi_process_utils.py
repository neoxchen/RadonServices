from multiprocessing import Pool

from tqdm import tqdm


def parallel_process(function, inputs, worker_count=32, chunk_size=16):
    with Pool(processes=worker_count) as pool:
        pool: Pool
        results = []
        for result in tqdm(pool.imap(function, inputs, chunksize=chunk_size), total=len(inputs)):
            results.append(result)
    return results


def _plus_one(args) -> int:
    return args[0] + 1


if __name__ == "__main__":
    print(parallel_process(_plus_one, [(a,) for a in range(100000)], worker_count=32))
