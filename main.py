import aiohttp
import asyncio
import time
from typing import Dict, Union, List, Any
from dotenv import load_dotenv
import datetime
import os
import json
import uuid

load_dotenv()

SEMAPHORE_LIMIT = 10
semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

MESSAGES_DO_NOT_APPLY = (
    "FATAL:  password authentication failed for user",
)

class DataSource:
    def __init__(self, url: str, headers: Dict[str, str]) -> None:
        self.url = url
        self.headers = headers
        self.session = aiohttp.ClientSession()

    async def close(self):
        await self.session.close()

    async def show(self, data_source_id: Union[int, None] = None):
        url = f"{self.url}" if data_source_id is None else f"{self.url}/{data_source_id}"
        async with self.session.get(url, headers=self.headers) as response:
            if response.status == 200:
                return await response.json()
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=[],
                status=response.status,
                message=f"HTTP error occurred: {response.status} - {await response.text()}",
                headers=response.headers
            )

    async def drop(self, data_source: Dict):
        url = f"{self.url}/{data_source['id']}"
        async with self.session.delete(url, headers=self.headers, json=data_source) as response:
            if response.status == 204:
                print(f"Data Source: {data_source['name']} eliminado con éxito")
            else:
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=[],
                    status=response.status,
                    message=f"HTTP error occurred: {response.status} - {await response.text()}",
                    headers=response.headers
                )

    async def test(self, data_source_id: int):
        url = f"{self.url}/{data_source_id}/test"
        async with self.session.post(url, headers=self.headers) as response:
            if response.status == 200:
                return await response.json()
            raise aiohttp.ClientResponseError(
                status=response.status,
                message=f"HTTP error occurred: {response.status} - {await response.text()}",
            )

    async def create(self, data_source: Dict):
        async with self.session.post(self.url, headers=self.headers, json=data_source) as response:
            if response.status == 200:
                print(f"Data Source: {data_source['name']} creado con éxito")
            else:
                raise aiohttp.ClientResponseError(
                    status=response.status,
                    message=f"HTTP error occurred: {response.status} - {await response.text()}",
                )


async def reestore(url, headers, data_sources: List[Dict[str, Any]]):
    response = DataSource(url, headers)
    tasks = [response.create(data_source) for data_source in data_sources]
    await asyncio.gather(*tasks)
    await response.close()


async def conversion_data(data):
    return {
        "name": data["name"],
        "type": data["type"],
        "options": data["options"]
    }


async def conversion_all_data(data_sources):
    tasks = [conversion_data(dt) for dt in data_sources]
    results = await asyncio.gather(*tasks)
    return [result for result in results if result is not None]


async def get_data_sources(url: str, headers: Dict[str, str], mode: str, db_name: Union[str, None] = None, data_sources_ids: Union[List[str], None] = None):
    response = DataSource(url, headers)
    data_sources = await response.show()

    tasks = []
    if mode == "test":
        print("=" * 40)
        print("Lista de Data Sources:")
        print("=" * 40)
        tasks = [get_data_sources_by_failed_test(response, dt["id"]) for dt in data_sources]
    elif mode == "name":
        print("=" * 40)
        is_countries = confirm_action("Es para los 3 paises?")
        print("Lista de Data Sources:")
        print("=" * 40)
        tasks = [get_data_sources_by_dbname(response, dt["id"], db_name, is_countries) for dt in data_sources]
    elif mode == "id":
        print("=" * 40)
        print("Lista de Data Sources:")
        print("=" * 40)
        tasks = [get_data_sources_by_id(response, data_source_id) for data_source_id in data_sources_ids]

    results = await asyncio.gather(*tasks)
    await response.close()

    return [result for result in results if result is not None]


async def get_data_sources_by_dbname(response: DataSource, data_source_id: int, db_name: str, is_countries: str) -> Union[int, None]:
    async with semaphore:
        data_source = await response.show(data_source_id)
        if is_countries == "s":
            for country in ("br-", "co-", "mx-"):
                if data_source.get("options", {}).get("dbname") == country + db_name:
                    print(f"{data_source['name']} - {data_source['id']}")
                    return data_source
        elif is_countries == "n":
            if data_source.get("options", {}).get("dbname") == db_name:
                print(f"{data_source['name']} - {data_source['id']}")
                return data_source
    return None


async def get_data_sources_by_failed_test(response: DataSource, data_source_id: int) -> Union[int, None]:
    async with semaphore:
        data_source = await response.show(data_source_id)
        test = await response.test(data_source_id)
        if not test["ok"]:
            is_valid_message = all(not test["message"].startswith(message) for message in MESSAGES_DO_NOT_APPLY)
            if is_valid_message:
                print(f"{data_source['name']} - {data_source['id']}")
                return data_source
    return None


async def get_data_sources_by_id(response: DataSource, data_source_id: int) -> Union[int, None]:
    async with semaphore:
        data_source = await response.show(data_source_id)
        print(f"{data_source['name']} - {data_source['id']}")
        return data_source


async def delete_data_sources(response: DataSource, data_sources: List[Dict]):
    tasks = [response.drop(data_source) for data_source in data_sources]
    await asyncio.gather(*tasks)
 

def get_env_config(enviroment):
    if enviroment == "d":
        return os.getenv("DEV_REDASH_URL"), os.getenv("DEV_API_KEY")
    elif enviroment == "p":
        return os.getenv("PROD_REDASH_URL"), os.getenv("PROD_API_KEY")
    clear_terminal()
    print("Opción inválida, por favor intente nuevamente.")
    print("=" * 40)
    return None, None

def backup_data(data_sources):
    os.makedirs("temp", exist_ok=True)
    backup_file = f"temp/{str(uuid.uuid4())[:8]}_{datetime.date.today()}.json"
    with open(backup_file, "w") as backup:
        backup.write(json.dumps(data_sources))
    return backup_file

def confirm_action(message):
    options = "(Sí: S/s, No: N/n)"
    if "test fallido" in message or message.startswith("Eliminar Data Sources de la DB"):
        options = "(Sí: S/s, No: N/n, Eliminar por ID: I/i)"
    print("=" * 40)
    print(message)
    print(options)
    return input("Opción: ").lower()

def menu(options):
    print("=" * 40)
    for i, option in enumerate(options, 1):
        print(f"{i}. {option}")
    return input("Opción: ")

async def handle_deletion(url, headers, enviroments, enviroment):
    loop = asyncio.get_event_loop()
    delete_options = [
        "Por nombre de base de datos (sin nomenclatura de país)",
        "Por test fallido",
        "Por ID"
    ]
    option = int(menu(delete_options))
    
    if option == 1:
        db_name = input("Nombre de la DB: ").replace(" ", "")
        print(f"Data Sources para la DB: {db_name} en {enviroments[enviroment]}")
        data_sources = await get_data_sources(url, headers, db_name=db_name, mode="name")
        print("=" * 40)
        if data_sources and not len(data_sources) == 0:
            is_valid = confirm_action(f"Eliminar Data Sources de la DB: {db_name}?")
            if is_valid == "s":
                backup_data(data_sources)
                await delete_data_sources(DataSource(url, headers), data_sources)
            elif confirm == "i":
                await delete_by_id(url, headers, enviroments, enviroment)
        else:
            print(f"No existen DT relacionados a la DB: {db_name}")

    elif option == 2:
        data_sources = await get_data_sources(url, headers, mode="test")
        print("=" * 40)
        confirm = confirm_action(f"Eliminar Data Sources con test fallido en {enviroments[enviroment]}?")
        if data_sources and confirm == "s":
            backup_data(data_sources)
            await delete_data_sources(DataSource(url, headers), data_sources)
        elif confirm == "i":
            await delete_by_id(url, headers, enviroments, enviroment)

    elif option == 3:
        await delete_by_id(url, headers, enviroments, enviroment)
    else:
        print("=" * 40)
        print("Opción inválida, por favor intente nuevamente.")
        await handle_deletion(url, headers, enviroments, enviroment)

async def delete_by_id(url, headers, enviroments, enviroment):
    ids = input(f"IDs de Data Sources a eliminar en {enviroments[enviroment]}: ").split(" ")
    data_sources = await get_data_sources(url, headers, mode="id", data_sources_ids=ids)
    if data_sources and confirm_action(f"Eliminar Data Sources por ID en {enviroments[enviroment]}?") == "s":
        backup_data(data_sources)
        await delete_data_sources(DataSource(url, headers), data_sources)
        print(f"Data Sources eliminados: {data_sources}")

def clear_terminal():
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    loop = asyncio.get_event_loop()
    while True:
        print("Menú de eliminación de Datasources de Redash")
        menu_options = ["Eliminar DT de redash", "Restaurar eliminación anterior", "Salir"]
        option = menu(menu_options)
        print("=" * 40)
        
        if option == "1":
            start = time.time()
            print("¿En que entorno desea realizar la acción anterior?")
            enviroment = input("¿Development (D/d) o Producción (P/p)?: ").lower()
            url, api_key = get_env_config(enviroment)
            if url and api_key:
                enviroments = {"p": "PRODUCTION", "d": "DEVELOP"}
                url += "/api/data_sources"
                headers = {"Authorization": f"Key {api_key}"}
                loop.run_until_complete(handle_deletion(url, headers, enviroments, enviroment))
            end = time.time()
            print(f"Tiempo de ejecución: {round(start - end, 2)}")
        elif option == "2":
            start = time.time()
            print("¿En que entorno desea realizar la acción anterior?")
            enviroment = input("¿Development (D/d) o Producción (P/p)?: ").lower()
            url, api_key = get_env_config(enviroment)
            if url and api_key:
                url += "/api/data_sources"
                file = f"temp/{input('Nombre de archivo de backup en temp: ')}"
                try:
                    with open(file) as backup:
                        data_sources = json.loads(backup.read())
                    is_valid = confirm_action(f"¿Restaurar Data Sources del archivo {file}?")
                    if is_valid == "s":
                        loop.run_until_complete(reestore(url, {"Authorization": f"Key {api_key}"}, data_sources))
                    elif is_valid == "n":
                        exit()
                    else:
                        clear_terminal()
                        print("Opción inválida, por favor intente nuevamente.")
                        print("=" * 40)
                except Exception as e:
                    print(e)
            end = time.time()
            print(f"Tiempo de ejecución: {round(end - start, 2)}")
        elif option == "3":
            exit()
        else:
            clear_terminal()
            print("Opción inválida, por favor intente nuevamente.")
            print("=" * 40)


if __name__ == "__main__":
    main()