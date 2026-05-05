from __future__ import annotations
from typing import List, Mapping, Optional, Sequence 
import numpy as np 
from .config import GAConfig 
from .domain import Schedule 
from .evaluation import evaluate_usage_and_peak
import pygad


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

    def __init__(self, config: Optional[Mapping[str, object]] = None, blocklist_schedule_ids: Optional[List[str]] = None):
        if config is None:
            self.cfg = GAConfig()
        else:
            filtered_config = {key: value for key, value in config.items() if value is not None}
            self.cfg = GAConfig(**filtered_config)
        self.blocklist_schedule_ids = blocklist_schedule_ids if blocklist_schedule_ids is not None else []


    def _gene_space(self, schedules: Sequence[Schedule]) -> List[List[int]]:
        # Build gene_space per schedule: each gene space is limited by it's frequency
        # Unless the schedule is unsupported (either blocklisted, irregular or has frequency greater than 60 mins) in which case we set the gene space to be just 0 
        # so they remain unchanged in the GA but are still included in the fitness evaluation. Also constrain schedules with frequency > 60 mins to an hour to prevent
        # large shifts and huge gene spaces.

        min_start_times = [0] * len(schedules)
        max_start_times = [1] * len(schedules)
        mins_per_day = 1440

        for i, schedule in enumerate(schedules):
            # Fix the gene space so they're still included in the fitness eval but remain unshifted
            if schedule.is_unsupported():
                min_start_times[i] = schedule.start_time_mins
                max_start_times[i] = schedule.start_time_mins + 1

            # Limit gene space to only shift within the hour for the schedules which run less frequently
            elif schedule.frequency_minutes > 60:
                # Prevent any max_start_time from going beyond the day limit 
                max_start_times[i] = min(schedule.start_time_mins + 60, mins_per_day)
                min_start_times[i] = max_start_times[i] - 60

            # Gene space for the rest is just the frequency 
            else:
                max_start_times[i] = schedule.frequency_minutes

        return [list(range(min_start_time, max_start_time)) for min_start_time, max_start_time in zip(min_start_times, max_start_times)]
    

    def _initial_population(self, schedules: Sequence[Schedule], gene_space: List[List[int]]) -> np.ndarray:
        rng = np.random.default_rng(self.cfg.random_seed)
        seed = []

        # Add current start minutes as first solution to bias solution space towards current solution
        for i, schedule in enumerate(schedules):
            gs = gene_space[i]
            s = int(schedule.start_time_mins)
            seed.append(max(min(s, gs[-1]), gs[0]))
        pop = [seed]

        # Populate the rest of the initial population randomly within the gene space limits for each schedule
        for _ in range(self.cfg.sol_per_pop - 1):
            pop.append([gene_space[i][int(rng.integers(0, len(gene_space[i])))] for i in range(len(schedules))])
        return np.asarray(pop, dtype=int)
    
    def _blocklist(self):
        self.cfg.blocklist_schedule_ids = set(self.cfg.blocklist_schedule_ids)
        raise NotImplementedError("blocklist functionality not yet implemented")

    def fitness_fn(self, ga, solution, solution_idx):
        _, peak = evaluate_usage_and_peak(solution, self.schedules)
        return -float(peak)
        
    def solve(self, schedules: Sequence[Schedule]) -> tuple[Sequence[Schedule], List[int], float, np.ndarray]:
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
            assert start_times[i] >= gene_space[i][0] and start_times[i] <= gene_space[i][-1], f"Start time for schedule {schedule.schedule_id} is out of gene space bounds. Start time: {start_times[i]}, Gene space: {gene_space[i]}"
            if schedule.is_unsupported(): assert start_times[i] == schedule.start_time_mins, f"Unsupported schedule {schedule.schedule_id} should not have been shifted in the GA solution. {schedule.start_time_mins} != {start_times[i]}"
            elif schedule.start_time_mins != start_times[i]: 
                schedule.shifted = True
                schedule.start_time_mins = start_times[i]
            
        return schedules, start_times, peak_usage, usage, -initial_fitness