from asyncio import Semaphore, gather, get_event_loop
from typing import List, Set, Tuple

import aiofiles
import numpy as np
from aiocsv import AsyncDictReader, AsyncWriter


async def load_raw_data():
    async with aiofiles.open(f'raw.csv', 'r', encoding="utf-8-sig", newline="") as f:
        raw_list = []
        async for row in AsyncDictReader(f):
            raw_list.append(row)
        return raw_list


async def split_data():
    raw_data = await load_raw_data()
    # There are 10 pieces of data in each small list
    n = 10
    new_data = [raw_data[i:i+n] for i in range(0, len(raw_data), n)]
    return new_data


async def get_one(small_list: List):
    try:
        async with aiofiles.open(f'result.csv', 'a', encoding="utf-8-sig", newline="") as f:
            data = [[i['uservip_degree'], i['timestamp'], i['content'],
                     i['opername'], i['upcount']] for i in small_list]
            writer = AsyncWriter(f)
            await writer.writerows(data)

    except Exception as e:
        print(e)


async def trunks(sem: Semaphore, small_list: List):
    async with sem:
        await get_one(small_list)


async def get_group(sem: Semaphore, data: List) -> Tuple[Set[int], np.ndarray]:
    tasks = []
    for i in data:
        tasks.append(trunks(sem, i))
    await gather(*tasks)


async def main():
    new_data = await split_data()
    async with aiofiles.open(f'result.csv', 'w', encoding="utf-8-sig", newline="") as f:
        f.truncate()
        writer = AsyncWriter(f)
        await writer.writerow(['uservip_degree', 'timestamp', 'content', 'opername', 'upcount'])
    # When the parameter of `Semaphore` is set to 1, the number of records stored in `result.csv` is correct.
    # When it is set to 10, the number of records is incorrect.
    sem = Semaphore(10)
    await get_group(sem, new_data)
    pass

if __name__ == '__main__':
    get_event_loop().run_until_complete(main())
