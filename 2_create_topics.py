from kafka.admin import KafkaAdminClient, NewTopic
import sys
import os
from colorama import Fore, init
from topics import athlete_event_results, enriched_athlete_avg
# Ініціалізація кольорового логування
init(autoreset=True)

# Додаємо шлях до кореневої директорії (для доступу до configs.py)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Тепер імпортуємо конфігурацію
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# # Визначення нових топіків
num_partitions = 2
replication_factor = 1

# Створення топіків
new_topics = [
    NewTopic(name=athlete_event_results, num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(name=enriched_athlete_avg, num_partitions=num_partitions, replication_factor=replication_factor)
]

try:
    print(f"{Fore.CYAN}Creating topics: {athlete_event_results} and {enriched_athlete_avg}...")
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"{Fore.GREEN}Topics '{athlete_event_results}' and '{enriched_athlete_avg}' created successfully.")
except Exception as e:
    if "TopicExistsException" in str(e):
        print(f"{Fore.YELLOW}Topics already exist: {e}")
    else:
        print(f"{Fore.RED}An error occurred while creating topics: {e}")
finally:
    print(f"{Fore.CYAN}Closing Kafka Admin Client...")
    admin_client.close()
    print(f"{Fore.GREEN}Kafka Admin Client closed successfully.")