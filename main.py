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

    split_rule = [190, 174, 257, 225, 229, 234, 250, 239, 290, 268, 258, 251, 247, 215, 268, 288, 234, 260, 285, 276, 282, 285, 278, 274, 292, 283, 283, 290, 228,
                  285, 288, 296, 272, 272, 275, 268, 290, 293, 286, 269, 297, 279, 281, 271, 223, 173, 244, 137, 260, 194, 246, 268, 282, 289, 230, 258, 294, 229, 206, 283, 281, 263, 280, 246, 282, 287, 294, 285, 300, 297, 296, 294, 296, 293, 299, 297, 290, 226, 258, 253, 274, 290, 284, 295, 280, 291, 279, 300, 230, 283, 292, 288, 287, 216, 277, 275, 275, 297, 299, 247, 213, 88, 111, 101, 217, 96, 74, 11]
    assert len(raw_data) == 27558

    new_data = []
    start = 0
    for i in split_rule:
        new_data.append(raw_data[start:start+i])
        start += i

    split_len = []
    for i in new_data:
        split_len.append(len(i))
    assert split_rule == split_len
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
