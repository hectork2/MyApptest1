from time import perf_counter
from typing import Dict, Optional


class Profiler:
    """
    Manual profiler that accumulates timing data with named data points

    # Create a profiler context with an initial time point (common use case)
    profiler = Profiler(start="init_tower_build")
    # Initialize a time point named
    profiler.start("start_tower")
    # Store a datapoint named tower_build_time in timing_results measuring time since start_tower
    profiler.stop("tower_build_time", previous_time="start_tower")

    # Store a datapoint named init_tower_build in timing_results measuring time since start_tower
    profiler.stop("init_tower_build")

    # Logging Examples
    # 1. Include timing metadata in a logging statement
    logger.info("Tower successfully built", extra=profiler.metadata())
    # 2. Timing metadata merged with other extra fields
    logger.info("Tower successfully built", extra={**other_extra, **profiler.metadata()}
    # 3. Timing metadata log merged with other metadata fields under a named field
    logger.info(
        "Tower successfully built",
        extra={"metadata": {**other_metadata, **profiler.metadata(key="tower_build_times")}}
    )
    """

    def __init__(self, start: Optional[str] = None):
        # Map named time points to the time of their occurrence
        self.timing_map: Dict[str, float] = dict()
        self.timing_results: Dict[str, float] = dict()
        if start:
            self.start(start)

    def start(self, time_point_name):
        self.timing_map[time_point_name] = perf_counter()

    def stop(
        self,
        time_point: str,
        previous_time: Optional[str] = None,
        next_time: Optional[str] = None,
        aggregate_time_point: Optional[str] = None,
    ) -> None:
        if not previous_time:
            previous_time = time_point

        previous_time_value = self.timing_map.get(previous_time)

        if not previous_time_value:
            raise ValueError(f"Previous time of {previous_time} does not exist")

        next_time_value = perf_counter()
        # Stage the data
        if aggregate_time_point:
            if aggregate_time_point not in self.timing_results:
                self.timing_results[aggregate_time_point] = 0
            self.timing_results[aggregate_time_point] += next_time_value - previous_time_value
        else:
            self.timing_results[time_point] = next_time_value - previous_time_value
        if next_time:
            # Prepare the start time of the next time point
            self.timing_map[next_time] = next_time_value

    def metadata(self, key: Optional[str] = "metadata"):
        return {key: self.timing_results}


class Profile:
    def __init__(
        self,
        time_point: str,
        profiler: Optional[Profiler] = None,
        previous_time: Optional[str] = None,
        next_time: Optional[str] = None,
        aggregate_time_point: Optional[str] = None,
    ):
        if not profiler:
            # Ignore previous time, since there can be no previous history if we're instantiating
            previous_time = None
            profiler = Profiler()
        self.time_point = time_point
        self.previous_time = previous_time
        self.profiler = profiler
        self.next_time = next_time
        self.aggregate_time_point = aggregate_time_point

    def __enter__(self):
        if not self.previous_time:
            self.profiler.start(self.time_point)
        return self.profiler

    def __exit__(self, type, value, traceback):
        self.profiler.stop(
            self.time_point,
            previous_time=self.previous_time,
            next_time=self.next_time,
            aggregate_time_point=self.aggregate_time_point,
        )
