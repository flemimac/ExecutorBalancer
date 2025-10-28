
import asyncio
import httpx
import json
from typing import List, Dict


BASE_URL = "http://localhost:8000"


async def example_workflow():
    async with httpx.AsyncClient() as client:
        
        print("\n1. Создание исполнителей...")
        executors_data = [
            {"name": "Алексей Иванов", "parameters": {"skill": "senior", "region": "Moscow"}},
            {"name": "Мария Петрова", "parameters": {"skill": "middle", "region": "SPb"}},
            {"name": "Дмитрий Сидоров", "parameters": {"skill": "junior", "region": "Kazan"}},
        ]
        
        executor_ids = []
        for executor_data in executors_data:
            response = await client.post(f"{BASE_URL}/executors/", json=executor_data)
            if response.status_code == 200:
                executor = response.json()
                executor_ids.append(executor['id'])
                print(f"   [OK] Создан: {executor['name']} (ID: {executor['id']})")
        
        print("\n2. Создание заявок...")
        requests_data = {
            "requests": [
                {"type": "urgent", "customer": "Client A", "amount": 10000},
                {"type": "urgent", "customer": "Client B", "amount": 15000},
                {"type": "normal", "customer": "Client C", "amount": 5000},
                {"type": "normal", "customer": "Client D", "amount": 8000},
                {"type": "low", "customer": "Client E", "amount": 2000},
                {"type": "low", "customer": "Client F", "amount": 3000},
            ]
        }
        
        response = await client.post(
            f"{BASE_URL}/requests/bulk/",
            json=requests_data
        )
        
        if response.status_code == 200:
            print(f"   [OK] Создано {len(requests_data['requests'])} заявок")
        
        print("\n3. Распределение и обработка заявок...")
        total_processed = 0
        
        for i, executor_id in enumerate(executor_ids):
            processed = 0
            
            while True:
                response = await client.post(
                    f"{BASE_URL}/executors/{executor_id}/get-next-request"
                )
                
                if response.status_code == 200 and response.json() is not None:
                    request = response.json()
                    print(f"   -> {executors_data[i]['name']} получил заявку #{request['id']}")
                    await asyncio.sleep(0.5)
                    await client.post(f"{BASE_URL}/requests/{request['id']}/complete")
                    processed += 1
                    total_processed += 1
                else:
                    break
            
            print(f"   [OK] {executors_data[i]['name']} обработал {processed} заявок")
        
        print("\n4. Динамическое добавление нового исполнителя...")
        new_executor = {
            "name": "Иван Новиков",
            "parameters": {"skill": "senior", "region": "Novosibirsk"}
        }
        
        response = await client.post(f"{BASE_URL}/executors/", json=new_executor)
        if response.status_code == 200:
            new_executor_data = response.json()
            print(f"   [OK] Добавлен новый исполнитель: {new_executor_data['name']}")
            
            new_processed = 0
            while True:
                response = await client.post(
                    f"{BASE_URL}/executors/{new_executor_data['id']}/get-next-request"
                )
                if response.status_code == 200 and response.json() is not None:
                    request = response.json()
                    await client.post(f"{BASE_URL}/requests/{request['id']}/complete")
                    new_processed += 1
                else:
                    break
            print(f"   [OK] {new_executor['name']} обработал {new_processed} заявок")
        
        print("\n5. Статистика распределения...")
        response = await client.get(f"{BASE_URL}/stats")
        if response.status_code == 200:
            stats = response.json()
            
            print(f"\n{'='*60}")
            print("ФИНАЛЬНАЯ СТАТИСТИКА")
            print(f"{'='*60}")
            print(f"Всего заявок: {stats['total_requests']}")
            print(f"Назначено: {stats['assigned_requests']}")
            print(f"Завершено: {stats['completed_requests']}")
            print(f"Активных исполнителей: {stats['active_executors']}")
            print(f"\nПогрешность распределения: {stats['distribution_error_percent']}%")
            print(f"\nЗагруженность по исполнителям:")
            for stat in stats['executor_stats']:
                print(f"  • {stat['name']}: {stat['actual_count']} заявок")
            print(f"{'='*60}\n")


async def quick_test():
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BASE_URL}/executors/",
            json={"name": "TestExecutor", "parameters": {}}
        )
        print(f"Создан исполнитель: {response.json()}")
        
        response = await client.post(
            f"{BASE_URL}/requests/",
            json={"parameters": {"test": "data"}}
        )
        print(f"Создана заявка: {response.json()}")
        
        response = await client.post(f"{BASE_URL}/executors/1/get-next-request")
        print(f"Получена заявка: {response.json()}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "quick":
        asyncio.run(quick_test())
    else:
        asyncio.run(example_workflow())

