import fetch
import producer


if __name__ == '__main__':
    all_recipes = fetch.get_recipes()
    if len(all_recipes) > 0:
        kafka_producer = producer.connect()
        for recipe in all_recipes:
            producer.publish(kafka_producer, 'raw_recipes', recipe.strip())
        if kafka_producer:
            kafka_producer.close()
