from http_status import HTTPStatus
from async_set import AsyncSet
from utils import category2dirname

from typing import AsyncGenerator, Optional

from rich.progress import (
    Progress,
    SpinnerColumn,
    TimeElapsedColumn,
    MofNCompleteColumn,
    TaskID,
)

import requests
import json

import logging
import os

import asyncio
import aiohttp

NUM_MAPPERS = 1
NUM_FETCHERS = 3
NUM_DOWNLOADERS = 10

# Setup logger
logging.basicConfig(
    filename = "downloader.log",
    level = logging.INFO,
    format = "%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)

# MediaWiki Endpoints
LIST_SUBCATEGORIES_ENDPOINT = "https://commons.wikimedia.org/w/api.php?action=query&list=categorymembers&cmtitle={title}&cmtype=subcat&format=json&utf8"
LIST_IMAGES_ENDPOINT = "https://commons.wikimedia.org/w/api.php?action=query&generator=categorymembers&gcmtitle={title}&gcmlimit=max&gcmtype=file&prop=imageinfo&iiprop=url&format=json&utf8"

# The maximum number of pages 
MAX_PAGES = 50

async def request(url: str, *, session: aiohttp.ClientSession) -> tuple[Optional[dict], int]:
    """Asynchronously makes a GET request to the specified URL."""
    async with session.get(url) as response:
        status = response.status

        if status == HTTPStatus.OK:
            data = await response.read()
            contents = json.loads(data)
        else:
            contents = None

        return contents, status

async def request_and_retry(url: str, *, session: aiohttp.ClientSession, retries: int = -1) -> Optional[dict]:
    """Asynchronously makes a GET request to the specified URL and retries if the request fails."""
    failures = 0
    
    while retries == -1 or failures < retries:
        contents, status = await request(url, session = session)
        
        if status == HTTPStatus.OK:
            return contents
        elif status == HTTPStatus.TOO_MANY_REQUESTS:
            logging.warning("Too many requests when accessing %s. Retrying in 5 seconds..." % url)
            await asyncio.sleep(5)
            failures += 1
        else:
            logging.error("Failed to make the request to %s. Status code: %d." % (url, status))
    
    logging.error("Failed to make the request to %s after %d retries." % (url, retries))

async def get_subcategories_no_continue(url: str, title: str, *, session: aiohttp.ClientSession) -> AsyncGenerator[None, str]:
    """Asynchronously fetches the subcategories of the specified category, given the request URL. Does not handle continuation."""
    contents = await request_and_retry(url, session = session)
    
    # TODO: KeyError: 'query'
    if "query" not in contents:
        logging.error("Got malformed response when listing subcategories of \"%s\".\n%s" % (title, json.dumps(contents, indent = 4)))
        return
    
    should_continue = "continue" in contents
    cmcontinue = contents["continue"]["cmcontinue"] if should_continue else None
    
    for subcategory in contents["query"]["categorymembers"]:
        yield subcategory["title"], should_continue, cmcontinue
    
async def get_subcategories(title: str, *, session: aiohttp.ClientSession) -> AsyncGenerator[None, str]:
    """Asynchronously lists all immediate subcategories of the specified category."""
    endpoint = LIST_SUBCATEGORIES_ENDPOINT.format(title = title)
    
    should_continue = False
    cmcontinue = None
    
    async for subcategory, should_continue, cmcontinue in get_subcategories_no_continue(endpoint, title, session = session):
        yield subcategory
    
    while should_continue:
        continuation_endpoint = endpoint + "&cmcontinue=" + cmcontinue
        
        async for subcategory, should_continue, cmcontinue in get_subcategories_no_continue(continuation_endpoint, title, session = session):
            yield subcategory

async def map_categories(title: str,
                         category_queue: asyncio.Queue,
                         unmapped_category_queue: asyncio.Queue,
                         seen_categories: AsyncSet,
                         *,
                         output_path: str,
                         mapper_id: int,
                         progress: Progress, task: TaskID):
    """Asynchronously maps all recursive subcategories of the specified category, filling them into the provided queue."""
    total = 1
    
    async with aiohttp.ClientSession() as session:
        while not unmapped_category_queue.empty():
            try:
                title, dirpath = unmapped_category_queue.get_nowait()
                progress.update(task, advance = 1, total = total)
                
                async for subcategory in get_subcategories(title, session = session):
                    is_new_category = await seen_categories.add_if_missing(subcategory)
                
                    if is_new_category:
                        subdirpath = os.path.join(dirpath, category2dirname(subcategory))
                        
                        await category_queue.put((subcategory, subdirpath))
                        await unmapped_category_queue.put((subcategory, subdirpath))
                        
                        total += 1
                        progress.update(task, advance = 0, total = total)
                    else:
                        logging.debug("Loop detected at %s." % subcategory)
                    
                unmapped_category_queue.task_done()
            except KeyboardInterrupt:
                return
            except Exception as e:
                logging.error("A task exited after it encountered an unexpected error while mapping categories: %s" % e)
    
    if mapper_id == 0:
        num_categories = await seen_categories.size()
        logging.debug("Discovered %d categories." % num_categories)
        
        progress.update(task, visible = False, refresh = True)

async def spawn_category_mappers(title: str,
                                 category_queue: asyncio.Queue,
                                 seen_categories: AsyncSet,
                                 *,
                                 output_path: str,
                                 num_mappers: int,
                                 progress: Progress, task: TaskID):
    """Spawns the asynchronous category mapper tasks."""
    dirpath = os.path.join(output_path, category2dirname(title))

    unmapped_category_queue = asyncio.Queue()
    
    await category_queue.put((title, dirpath))
    await unmapped_category_queue.put((title, dirpath))
    
    tasks = []
    
    for mapper_id in range(num_mappers):
        task = asyncio.create_task(map_categories(title,
                                                  category_queue,
                                                  unmapped_category_queue,
                                                  seen_categories,
                                                  output_path = output_path,
                                                  mapper_id = mapper_id,
                                                  progress = progress, task = task))
        tasks.append(task)
    
    return tasks

async def get_images_no_continue(url: str, *, session: aiohttp.ClientSession) -> AsyncGenerator[None, str]:
    """Asynchronously fetches the images of the specified category, given the request URL. Does not handle continuation."""
    contents = await request_and_retry(url, session = session)
    
    # There are no images on this page
    if "query" not in contents:
        return
    
    should_continue = "continue" in contents
    
    if "continue" in contents:
        should_continue = True
        
        if "gcmcontinue" in contents["continue"]:
            continuation_param = "gcmcontinue"
            continuation_value = contents["continue"]["gcmcontinue"]
        elif "iistart" in contents["continue"]:
            continuation_param = "iistart"
            continuation_value = contents["continue"]["iistart"]
    else:
        should_continue = False
        continuation_param = None
        continuation_value = None
    
    for page_id, page in contents["query"]["pages"].items():
        title = page["title"]
        
        # TODO: Check this
        if len(page["imageinfo"]) > 1:
            logging.warning("Found more than one image for page \"%s\"." % title)
            
        url = page["imageinfo"][0]["url"]
        yield title, url, should_continue, continuation_param, continuation_value
    
async def get_images(title: str, *, session: aiohttp.ClientSession) -> AsyncGenerator[None, str]:
    """Asynchronously lists all images under the specified categories."""
    endpoint = LIST_IMAGES_ENDPOINT.format(title = title)
    
    should_continue = False
    continuation_param = None
    continuation_value = None
    
    async for image_title, image_url, should_continue, continuation_param, continuation_value in get_images_no_continue(endpoint, session = session):
        yield image_title, image_url
    
    while should_continue:
        continuation_endpoint = endpoint + f"&{continuation_param}={continuation_value}"
        
        async for image_title, image_url, should_continue, continuation_param, continuation_value in get_images_no_continue(continuation_endpoint, session = session):
            yield image_title, image_url
            
async def fetch_images(category_queue: asyncio.Queue, download_queue: asyncio.Queue, seen_images: AsyncSet,
                       *,
                       progress: Progress, task: TaskID):
    """Asynchronously fetches images from the specified category and submit them to the downloaders."""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                title, dirpath = await category_queue.get()
                
                # Received stop signal from the mapper tasks
                if title is None:
                    break
                
                async for image_title, image_url in get_images(title, session = session):
                    is_new_image = await seen_images.add_if_missing(image_url)
                
                    if is_new_image:
                        image_name = image_title.replace("File:", "")
                        image_path = os.path.join(dirpath, image_name)
                    
                        await download_queue.put((image_url, image_path))
                        
                        total = await seen_images.size()
                        progress.update(task, total = total)
                    else:
                        logging.debug("Skipping duplicate image \"%s\"." % image_title)
                
                category_queue.task_done()
            except KeyboardInterrupt:
                return
            except Exception as e:
                logging.error("A task exited after it encountered an unexpected error while downloading images: %s" % e)

async def spawn_image_fetchers(category_queue: asyncio.Queue, download_queue: asyncio.Queue, seen_images: AsyncSet,
                               *,
                               num_fetchers: int,
                               progress: Progress, task: TaskID):
    """Spawns the asynchronous image fetcher tasks."""
    image_fetchers = []
    
    for _ in range(num_fetchers):
        image_fetcher = asyncio.create_task(fetch_images(category_queue, download_queue, seen_images,
                                                         progress = progress, task = task))
        image_fetchers.append(image_fetcher)
    
    return image_fetchers

async def download_image(image_url: str, image_path: str, *, session: aiohttp.ClientSession):
    """Asynchronously downloads the image from the specified URL to the specified path."""
    os.makedirs(os.path.dirname(image_path), exist_ok = True)
    
    async with session.get(image_url) as response:
        if response.status == HTTPStatus.OK:
            with open(image_path, "wb") as file:
                file.write(await response.read())
        else:
            logging.error("Failed to download image from %s. Status code: %d." % (image_url, response.status))

async def download_images(download_queue: asyncio.Queue, seen_images: AsyncSet,
                          *,
                          progress: Progress, task: TaskID):
    async with aiohttp.ClientSession() as session:
        while True:
            image_url, image_path = await download_queue.get()
            
            # Received the stop signal from the fetcher tasks
            if image_url is None:
                break
            
            if not os.path.exists(image_path):
                await download_image(image_url, image_path, session = session)
            
            total = await seen_images.size()
            progress.update(task, advance = 1, total = total)

async def spawn_image_downloaders(download_queue: asyncio.Queue, seen_images: AsyncSet,
                                  *,
                                  num_downloaders: int,
                                  progress: Progress, task: TaskID):
    """Spawns the asynchronous image downloader tasks."""
    image_downloaders = []
    
    for _ in range(num_downloaders):
        image_downloader = asyncio.create_task(download_images(download_queue, seen_images,
                                                               progress = progress, task = task))
        image_downloaders.append(image_downloader)
    
    return image_downloaders

async def main():
    # title = input("Enter the name of the category: ")
    
    categories_filepath = input("Enter the path to a text file containing the category titles, one on each line: ")
    
    titles = []
    
    with open(categories_filepath, 'r') as file:
        for line in file:
            title = "Category:" + line.strip()
            titles.append(title)
    
    output_path = input("Enter the path to the output directory (default: download): ")
    
    if output_path == "":
        output_path = "download"

    progress = Progress(
        SpinnerColumn(),
        *Progress.get_default_columns(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        transient = True,
        refresh_per_second = 10,
    )
    
    category_queue = asyncio.Queue()
    download_queue = asyncio.Queue()
    
    seen_categories = AsyncSet(titles)
    seen_images = AsyncSet()
    
    with progress:
        mapper_tasks = []
        
        for title in titles:
            task = progress.add_task("[blue]Mapping \"%s\"..." % title, total = None)
            category_mappers = await spawn_category_mappers(title, category_queue, seen_categories,
                                                            output_path = output_path,
                                                            num_mappers = NUM_MAPPERS,
                                                            progress = progress, task = task)
            mapper_tasks.extend(category_mappers)
        
        # Spawn image fetchers and downloaders
        task = progress.add_task("[green]Downloading images...", total = None)
        
        fetcher_tasks = await spawn_image_fetchers(category_queue, download_queue, seen_images,
                                                   num_fetchers = NUM_FETCHERS,
                                                   progress = progress, task = task)
        
        downloader_tasks = await spawn_image_downloaders(download_queue, seen_images,
                                                         num_downloaders = NUM_DOWNLOADERS,
                                                         progress = progress, task = task)
        
        await asyncio.gather(*mapper_tasks)
    
        num_categories = await seen_categories.size()
        logging.info("Found %d categories." % num_categories)
        
        # Submit stop signals to the fetchers
        for _ in range(NUM_FETCHERS):
            await category_queue.put((None, None))
            
        await asyncio.gather(*fetcher_tasks)
        
        logging.info("Submitted all images to the download queue.")
        
        # Submit stop signals to the downloaders
        for _ in range(NUM_DOWNLOADERS):
            await download_queue.put((None, None))
            
        await asyncio.gather(*downloader_tasks)
        
        num_images = await seen_images.size()
        logging.info("Finished downloading %d images." % num_images)
    
if __name__ == "__main__":
    asyncio.run(main())