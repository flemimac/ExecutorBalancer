
import asyncio
import httpx
import random
import json


BASE_URL = "http://localhost:8000"


async def create_executors():
    async with httpx.AsyncClient() as client:
        executors = [
            {"name": "Executor-1", "parameters": {"priority": "high", "region": "Moscow"}},
            {"name": "Executor-2", "parameters": {"priority": "medium", "region": "SPb"}},
            {"name": "Executor-3", "parameters": {"priority": "low", "region": "Kazan"}},
        ]
        
        for executor in executors:
            response = await client.post(f"{BASE_URL}/executors/", json=executor)
            if response.status_code == 200:
                print(f"[OK] Создан исполнитель: {executor['name']}")
            else:
                print(f"[ERROR] Ошибка создания исполнителя {executor['name']}: {response.text}")


async def create_requests():
    async with httpx.AsyncClient(timeout=60.0) as client:
        total_requests = 15000
        batch_size = 1000
        
        for i in range(0, total_requests, batch_size):
            requests_batch = []
            for j in range(i, min(i + batch_size, total_requests)):
                request_data = {
                    "id": j + 1,
                    "type": random.choice(["urgent", "normal", "low"]),
                    "customer_id": random.randint(1000, 9999),
                    "value": random.randint(100, 50000),
                    "region": random.choice(["Moscow", "SPb", "Kazan", "Novosibirsk"])
                }
                requests_batch.append(request_data)
            
            response = await client.post(
                f"{BASE_URL}/requests/bulk/",
                json={"requests": requests_batch}
            )
            
            if response.status_code == 200:
                print(f"[OK] Создано заявок: {i + len(requests_batch)}/{total_requests}")
            else:
                print(f"[ERROR] Ошибка: {response.text}")


async def simulate_work():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/executors/")
        executors = response.json()
        
        print(f"\n Симуляция обработки заявок...")
        
        for executor in executors:
            if not executor['is_active']:
                continue
            
            executor_id = executor['id']
            processed = 0
            
            while True:
                response = await client.post(
                    f"{BASE_URL}/executors/{executor_id}/get-next-request"
                )
                
                if response.status_code == 200 and response.json() is not None:
                    request = response.json()
                    await asyncio.sleep(0.1)  
                    
                    await client.post(f"{BASE_URL}/requests/{request['id']}/complete")
                    processed += 1
                else:
                    break
            
            print(f"[OK] {executor['name']} обработал {processed} заявок")


async def main():
    print(" Генерация тестовых данных\n")
    
    print("1. Создание исполнителей")
    await create_executors()
    
    print("\n2. Создание 10000 заявок (это может занять несколько минут)...")
    await create_requests()
    
    print("\n3. Симуляция распределения и обработки...")
    await simulate_work()
    
    print("\n4. Получение статистики")
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/stats")
        stats = response.json()
        
        print(f"\n{'='*50}")
        print("СТАТИСТИКА РАСПРЕДЕЛЕНИЯ")
        print(f"{'='*50}")
        print(f"Всего заявок: {stats['total_requests']}")
        print(f"Ожидающих: {stats['pending_requests']}")
        print(f"Назначено: {stats['assigned_requests']}")
        print(f"Завершено: {stats['completed_requests']}")
        print(f"Активных исполнителей: {stats['active_executors']}")
        print(f"\nПогрешность распределения: {stats['distribution_error_percent']}%")
        print(f"\nЗагруженность исполнителей:")
        for stat in stats['executor_stats']:
            print(f"  - {stat['name']}: {stat['actual_count']} заявок")
        print(f"{'='*50}\n")


if __name__ == "__main__":
    asyncio.run(main())

