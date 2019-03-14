#include <iostream>
#include <thread>
#include <atomic>
#include <Processors/IProcessor.h>
#include <Processors/ISource.h>
#include <Processors/ISink.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/ForkProcessor.h>
#include <Processors/LimitTransform.h>
#include <Processors/QueueBuffer.h>
#include <Processors/Executors/SequentialPipelineExecutor.h>
#include <Processors/Executors/ParallelPipelineExecutor.h>
#include <Processors/printPipeline.h>

#include <Columns/ColumnsNumber.h>
#include <Common/ThreadPool.h>
#include <Common/EventCounter.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>


using namespace DB;


class NumbersSource : public ISource
{
public:
    String getName() const override { return "Numbers"; }

    NumbersSource(UInt64 start_number, UInt64 step, UInt64 block_size, unsigned sleep_useconds)
            : ISource(Block({ColumnWithTypeAndName{ ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number" }})),
              current_number(start_number), step(step), block_size(block_size), sleep_useconds(sleep_useconds)
    {
    }

private:
    UInt64 current_number = 0;
    UInt64 step;
    UInt64 block_size;
    unsigned sleep_useconds;

    Chunk generate() override
    {
        usleep(sleep_useconds);

        MutableColumns columns;
        columns.emplace_back(ColumnUInt64::create());

        for (UInt64 i = 0; i < block_size; ++i, current_number += step)
            columns.back()->insert(Field(current_number));

        return Chunk(std::move(columns), block_size);
    }
};

class PrintSink : public ISink
{
public:
    String getName() const override { return "Print"; }

    PrintSink(String prefix, Block header)
            : ISink(std::move(header)),
              prefix(std::move(prefix))
    {
    }

private:
    String prefix;
    WriteBufferFromFileDescriptor out{STDOUT_FILENO};
    FormatSettings settings;

    void consume(Chunk chunk) override
    {
        size_t rows = chunk.getNumRows();
        size_t columns = chunk.getNumColumns();

        for (size_t row_num = 0; row_num < rows; ++row_num)
        {
            writeString(prefix, out);
            for (size_t column_num = 0; column_num < columns; ++column_num)
            {
                if (column_num != 0)
                    writeChar('\t', out);
                getPort().getHeader().getByPosition(column_num).type->serializeAsText(*chunk.getColumns()[column_num], row_num, out, settings);
            }
            writeChar('\n', out);
        }

        out.next();
    }
};

template<typename TimeT = std::chrono::milliseconds>
struct measure
{
    template<typename F, typename ...Args>
    static typename TimeT::rep execution(F&& func, Args&&... args)
    {
        auto start = std::chrono::steady_clock::now();
        std::forward<decltype(func)>(func)(std::forward<Args>(args)...);
        auto duration = std::chrono::duration_cast< TimeT>
                (std::chrono::steady_clock::now() - start);
        return duration.count();
    }
};

int main(int, char **)
try
{
    ThreadStatus thread_status;

    registerAggregateFunctions();
    auto & factory = AggregateFunctionFactory::instance();

    auto execute_one_stream = [&](String msg, ThreadPool * pool)
    {
        std::cerr << msg << "\n";

        auto source1 = std::make_shared<NumbersSource>(0, 1, 10, 0);
        auto source2 = std::make_shared<NumbersSource>(0, 1, 10, 0);
        auto source3 = std::make_shared<NumbersSource>(0, 1, 10, 0);

        auto limit1 = std::make_shared<LimitTransform>(source1->getPort().getHeader(), 100, 0);
        auto limit2 = std::make_shared<LimitTransform>(source2->getPort().getHeader(), 100, 0);
        auto limit3 = std::make_shared<LimitTransform>(source3->getPort().getHeader(), 100, 0);

        auto resize = std::make_shared<ResizeProcessor>(source1->getPort().getHeader(), 3, 1);

        AggregateDescriptions aggregate_descriptions(1);

        DataTypes sum_types = { std::make_shared<DataTypeUInt64>() };
        aggregate_descriptions[0].function = factory.get("sum", sum_types);
        aggregate_descriptions[0].arguments = {0};

        bool overflow_row = false; /// Without overflow row.
        size_t max_rows_to_group_by = 0; /// All.
        size_t group_by_two_level_threshold = 0; /// Always single level.
        size_t group_by_two_level_threshold_bytes = 0; /// Always single level.
        size_t max_bytes_before_external_group_by = 0; /// No external group by.

        Aggregator::Params params(
                source1->getPort().getHeader(),
                {0},
                aggregate_descriptions,
                overflow_row,
                max_rows_to_group_by,
                OverflowMode::THROW,
                nullptr, /// No compiler
                0, /// min_count_to_compile
                group_by_two_level_threshold,
                group_by_two_level_threshold_bytes,
                max_bytes_before_external_group_by,
                false, /// empty_result_for_aggregation_by_empty_set
                "", /// tmp_path
                1 /// max_threads
            );

        auto agg_params = std::make_shared<AggregatingTransformParams>(params, /* final =*/ false);
        auto merge_params = std::make_shared<AggregatingTransformParams>(params, /* final =*/ true);
        auto aggregating = std::make_shared<AggregatingTransform>(source1->getPort().getHeader(), agg_params);
        auto merging = std::make_shared<MergingAggregatedTransform>(aggregating->getOutputs().front().getHeader(), merge_params, 4);
        auto sink = std::make_shared<PrintSink>("", merging->getOutputPort().getHeader());

        connect(source1->getPort(), limit1->getInputPort());
        connect(source2->getPort(), limit2->getInputPort());
        connect(source3->getPort(), limit3->getInputPort());

        auto it = resize->getInputs().begin();
        connect(limit1->getOutputPort(), *(it++));
        connect(limit2->getOutputPort(), *(it++));
        connect(limit3->getOutputPort(), *(it++));

        connect(resize->getOutputs().front(), aggregating->getInputs().front());
        connect(aggregating->getOutputs().front(), merging->getInputPort());
        connect(merging->getOutputPort(), sink->getPort());

        std::vector<ProcessorPtr> processors = {source1, source2, source3,
                                                limit1, limit2, limit3,
                                                resize, aggregating, merging, sink};
//        WriteBufferFromOStream out(std::cout);
//        printPipeline(processors, out);

        PipelineExecutor executor(processors, pool);
        executor.execute();
    };

    auto execute_mult_streams = [&](String msg, ThreadPool * pool)
    {
        std::cerr << msg << "\n";

        auto source1 = std::make_shared<NumbersSource>(0, 1, 10, 0);
        auto source2 = std::make_shared<NumbersSource>(0, 1, 10, 0);
        auto source3 = std::make_shared<NumbersSource>(0, 1, 10, 0);

        auto limit1 = std::make_shared<LimitTransform>(source1->getPort().getHeader(), 100, 0);
        auto limit2 = std::make_shared<LimitTransform>(source2->getPort().getHeader(), 100, 0);
        auto limit3 = std::make_shared<LimitTransform>(source3->getPort().getHeader(), 100, 0);

        AggregateDescriptions aggregate_descriptions(1);

        DataTypes sum_types = { std::make_shared<DataTypeUInt64>() };
        aggregate_descriptions[0].function = factory.get("sum", sum_types);
        aggregate_descriptions[0].arguments = {0};

        bool overflow_row = false; /// Without overflow row.
        size_t max_rows_to_group_by = 0; /// All.
        size_t group_by_two_level_threshold = 0; /// Always single level.
        size_t group_by_two_level_threshold_bytes = 0; /// Always single level.
        size_t max_bytes_before_external_group_by = 0; /// No external group by.

        Aggregator::Params params(
                source1->getPort().getHeader(),
                {0},
                aggregate_descriptions,
                overflow_row,
                max_rows_to_group_by,
                OverflowMode::THROW,
                nullptr, /// No compiler
                0, /// min_count_to_compile
                group_by_two_level_threshold,
                group_by_two_level_threshold_bytes,
                max_bytes_before_external_group_by,
                false, /// empty_result_for_aggregation_by_empty_set
                "", /// tmp_path
                1 /// max_threads
        );

        auto agg_params = std::make_shared<AggregatingTransformParams>(params, /* final =*/ false);
        auto merge_params = std::make_shared<AggregatingTransformParams>(params, /* final =*/ true);

        ManyAggregatedDataPtr data = std::make_unique<ManyAggregatedData>(3);

        auto aggregating1 = std::make_shared<AggregatingTransform>(source1->getPort().getHeader(), agg_params, data, 0, 4, 4);
        auto aggregating2 = std::make_shared<AggregatingTransform>(source1->getPort().getHeader(), agg_params, data, 1, 4, 4);
        auto aggregating3 = std::make_shared<AggregatingTransform>(source1->getPort().getHeader(), agg_params, data, 2, 4, 4);

        Processors merging_pipe = createMergingAggregatedMemoryEfficientPipe(
                aggregating1->getOutputs().front().getHeader(),
                merge_params,
                3, 2);

        auto sink = std::make_shared<PrintSink>("", merging_pipe.back()->getOutputs().back().getHeader());

        connect(source1->getPort(), limit1->getInputPort());
        connect(source2->getPort(), limit2->getInputPort());
        connect(source3->getPort(), limit3->getInputPort());

        connect(limit1->getOutputPort(), aggregating1->getInputs().front());
        connect(limit2->getOutputPort(), aggregating2->getInputs().front());
        connect(limit3->getOutputPort(), aggregating3->getInputs().front());

        auto it = merging_pipe.front()->getInputs().begin();
        connect(aggregating1->getOutputs().front(), *(it++));
        connect(aggregating2->getOutputs().front(), *(it++));
        connect(aggregating3->getOutputs().front(), *(it++));

        connect(merging_pipe.back()->getOutputs().back(), sink->getPort());

        std::vector<ProcessorPtr> processors = {source1, source2, source3,
                                                limit1, limit2, limit3,
                                                aggregating1, aggregating2, aggregating3, sink};

        processors.insert(processors.end(), merging_pipe.begin(), merging_pipe.end());
//        WriteBufferFromOStream out(std::cout);
//        printPipeline(processors, out);

        PipelineExecutor executor(processors, pool);
        executor.execute();
    };

    ThreadPool pool(4, 4, 10);
    std::vector<String> messages;
    std::vector<Int64> times;

    auto exec = [&](auto func, String msg, ThreadPool * pool)
    {
        auto time = measure<>::execution(func, msg, pool);
        messages.emplace_back(msg);
        times.emplace_back(time);
    };

    exec(execute_one_stream, "One stream, single thread", nullptr);
    exec(execute_one_stream, "One stream, multiple threads", &pool);

    exec(execute_mult_streams, "Multiple streams, single thread", nullptr);
    exec(execute_mult_streams, "Multiple streams, multiple threads", &pool);

    for (size_t i = 0; i < messages.size(); ++i)
        std::cout << messages[i] << " time: " << times[i] << " ms.";

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    throw;
}
