
import pandas as pd

data = {
    'type': ['urgent', 'normal', 'low', 'urgent', 'normal', 'low'] * 5,
    'customer_id': [1001, 1002, 1003, 1004, 1005, 1006] * 5,
    'value': [10000, 5000, 2000, 15000, 8000, 3000] * 5,
    'region': ['Moscow', 'SPb', 'Kazan', 'Moscow', 'SPb', 'Kazan'] * 5,
    'priority': ['high', 'medium', 'low', 'high', 'medium', 'low'] * 5
}

df = pd.DataFrame(data)

df.to_excel('sample_requests.xlsx', index=False)
print("[OK] Создан файл sample_requests.xlsx с 30 заявками")
print("\nСтруктура файла:")
print(df.head(10))

