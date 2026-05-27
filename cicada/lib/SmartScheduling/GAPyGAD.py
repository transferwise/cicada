from __future__ import annotations
from typing import List, Mapping, Optional, Sequence 
import numpy as np 
import pygad

from cicada.lib.SmartScheduling.config import GAConfig 
from cicada.lib.SmartScheduling.domain import Schedule 
from cicada.lib.SmartScheduling.evaluation import evaluate_usage_and_peak


class GAPyGADScheduler:
    """
    Genetic Algorithm Scheduler using PyGAD
    Args:
        config: Optional[GAConfig] : configuration for the genetic algorithm
    Returns:
        Schedule : optimized schedule for all schedules 


    Implementation Note: We only consider the regular schedules during fitness evaluation to aid simplicity as there are few irregular 
                         schedules. All regular schedules are fed into the scheduler however those on the blocklist will remain unchanged 
                         and are kept purely to ensure the fitness evaluation is accurate to the actual schedule.
                         
                         We cap the max shift of a schedule to within the hour to prevent large shifts for schedules that run daily.
    """

    def __init__(self, config: Optional[Mapping[str, object]] = None):
        if config is None:
            self.cfg = GAConfig()
        else:
            filtered_config = {key: value for key, value in config.items() if value is not None}
            self.cfg = GAConfig(**filtered_config)


    def _gene_space(self, schedules: Sequence[Schedule]) -> List[dict]:
        # Build gene_space per schedule: each gene space is limited by its frequency
        # Unless the schedule is unsupported (either blocklisted, irregular or has frequency greater than 60 mins),
        # in which case we fix the gene space so it remains unchanged in the GA but is still included in fitness evaluation.
        # Constrain schedules with frequency > 60 mins to an hour to prevent large shifts.

        gene_space = []
        mins_per_day = 1440

        for schedule in schedules:
            if schedule.is_unsupported():
                # Fix gene space to current start time (no shift allowed)
                gene_space.append({"low": schedule.start_time_mins, "high": schedule.start_time_mins})
            elif schedule.frequency_minutes > 60:
                # Shift within the hour, clamped to day limit
                high = min(schedule.start_time_mins + 59, mins_per_day)
                low = max(high - 59, 0)
                gene_space.append({"low": low, "high": high})
            else:
                # Shift within frequency range
                gene_space.append({"low": 0, "high": schedule.frequency_minutes - 1})

        return gene_space

    def _initial_population(self, schedules: Sequence[Schedule], gene_space: List[List[int]]) -> np.ndarray:
        rng = np.random.default_rng(self.cfg.random_seed)
        seed = []

        # Add current start minutes as first solution to bias solution space towards current solution
        for i, schedule in enumerate(schedules):
            gs = gene_space[i]
            s = int(schedule.start_time_mins)
            seed.append(max(min(s, gs["high"]), gs["low"]))
        pop = [seed]

        # Populate the rest of the initial population randomly within the gene space limits for each schedule
        for _ in range(self.cfg.sol_per_pop - 1):
            pop.append([int(rng.integers(gene_space[i]["low"], gene_space[i]["high"] + 1)) for i in range(len(schedules))])
        return np.asarray(pop, dtype=int)

    def fitness_fn(self, ga, solution, solution_idx):
        _, peak = evaluate_usage_and_peak(solution, self.schedules)
        return -float(peak)
        
    def solve(self, schedules: Sequence[Schedule]) -> tuple[Sequence[Schedule], List[int], float, np.ndarray, float]:
        self.schedules = schedules
        gene_space = self._gene_space(schedules)
        print("Successfully initialised gene space")
        
        initial_population = self._initial_population(schedules, gene_space)
        print("Created initial population. Current Solution Start Times:")
        print(initial_population[0])

        initial_fitness = self.fitness_fn(None, initial_population[0], 0)
        print("Initial population fitness (max usage):", -initial_fitness)

        ga = pygad.GA(
            num_generations=self.cfg.num_generations,
            sol_per_pop=self.cfg.sol_per_pop,
            num_parents_mating=self.cfg.num_parents_mating,
            num_genes=len(schedules),
            gene_type=int,
            gene_space=gene_space,
            mutation_percent_genes=self.cfg.mutation_percent_genes,
            fitness_func=self.fitness_fn,
            parent_selection_type=self.cfg.parent_selection_type,
            keep_elitism=self.cfg.keep_elitism,
            crossover_type=self.cfg.crossover_type,
            mutation_type=self.cfg.mutation_type,
            allow_duplicate_genes=True,
            initial_population=initial_population,
            random_seed=self.cfg.random_seed,
        )
        ga.run()
        
        best_solution, best_fitness, _ = ga.best_solution()
        start_times = [int(v) for v in best_solution]
        peak_usage = -float(best_fitness)

        print(f"Optimised for {self.cfg.num_generations} generations. Best Solution Start Times:")
        print(best_solution)

        usage, _ = evaluate_usage_and_peak(start_times, schedules)

        # Update schedule objects start_time_mins attribute based on GA solution
        for i, schedule in enumerate(schedules):
            if not (start_times[i] >= gene_space[i]["low"] and start_times[i] <= gene_space[i]["high"]):
                raise RuntimeError(f"Start time for schedule {schedule.schedule_id} is out of gene space bounds. Start time: {start_times[i]}, Gene space: {gene_space[i]}")
            if schedule.is_unsupported() and start_times[i] != schedule.start_time_mins:
                raise RuntimeError(f"Unsupported schedule {schedule.schedule_id} should not have been shifted in the GA solution. {schedule.start_time_mins} != {start_times[i]}")
            elif schedule.start_time_mins != start_times[i]:
                schedule.shifted = True
                schedule.start_time_mins = start_times[i]
            
        return schedules, start_times, peak_usage, usage, -initial_fitness