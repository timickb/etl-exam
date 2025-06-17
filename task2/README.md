# Обработка данных с помощью PySpark

Датасет: https://www.kaggle.com/datasets/chaudharisanika/smartphones-dataset

В DAG считается статистика по бренду: количество моделей и их средний рейтинг.

### Как выполнялось

1. Пишу PySpark скрипт, считающий аналитику по данным
2. В Object Storage загружаю исходный датасет, написанный скрипт и необходимые `.jar` файлы
3. Создаю кластер Yandex Data Proc, в нем создаю задание (см. `task_config.png`)
4. Запускаю задание, вижу записанный в Object Storage результат (см. `result.csv`)