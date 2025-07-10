from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # For consistent output ordering

    data = env.from_collection([
        "Hii I am Flink",
        "Flink is doing great",
        "Flink is best"
    ])

    word_counts = (
        data
        .flat_map(lambda line: line.lower().split(), output_type=Types.STRING())
        .filter(lambda word: len(word) > 2)
        .map(lambda word: (word, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
        .key_by(lambda x: x[0])
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    )

    # Print counts to terminal
    word_counts.print()

    env.execute("Word Count Print Job")


if _name_ == "_main_":
    main()
