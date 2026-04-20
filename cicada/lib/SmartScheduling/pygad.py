from __future__ import annotations
from typing import List, Mapping, Optional, Sequence 
import numpy as np 
from .config import GAConfig 
from .domain import Tap 
from .evaluation import evaluate_cpu_usage_and_peak
import pygad


class GAPyGADScheduler:
    """
    Genetic Algorithm Scheduler using PyGAD
    Args:
        config: Optional[GAConfig] : configuration for the genetic algorithm
    Returns:
        Schedule : optimized schedule for all taps 


    Implementation Note: We only consider the regular taps during fitness evaluation to aid simplicity as there are few irregular 
                         taps. All regular taps are fed into the scheduler however those on the blacklist will remain unchanged 
                         and are kept purely to ensure the fitness evaluation is accurate to the actual schedule.
                         
                         We cap the max shift of a tap to within the hour to prevent large shifts for taps that run daily.
    """

    def __init__(self, config: Optional[Mapping[str, object]] = None, blacklist_schedule_ids: Optional[List[str]] = None):
        if config is None:
            self.cfg = GAConfig()
        else:
            filtered_config = {key: value for key, value in config.items() if value is not None}
            self.cfg = GAConfig(**filtered_config)
        self.blacklist_schedule_ids = blacklist_schedule_ids if blacklist_schedule_ids is not None else []


    def _gene_space(self, taps: Sequence[Tap]) -> List[List[int]]:
        # Build gene_space per tap: each gene space is limited by it's frequency
        # Unless the tap is unsupported (either blacklisted, irregular or has frequency greater than 60 mins) in which case we set the gene space to be just 0 
        # so they remain unchanged in the GA but are still included in the fitness evaluation. Also constrain taps with frequency > 60 mins to an hour to prevent
        # large shifts and huge gene spaces.

        min_start_times = [0] * len(taps)
        max_start_times = [1] * len(taps)
        mins_per_day = 1440

        for i, tap in enumerate(taps):
            # Ignore any blacklist taps -> this fixes the gene space to be 0 so they're still included in the fitness eval but remain unshifted
            if tap.is_unsupported():
                pass

            # Limit gene space to only shift within the hour for the taps which run less frequently
            elif tap.frequency_minutes > 60:
                # Prevent any max_start_time from going beyond the day limit 
                max_start_times[i] = min(tap.start_time_mins + 60, mins_per_day)
                min_start_times[i] = max_start_times[i] - 60

            # Gene space for the rest is just the frequency 
            else:
                min_start_times[i] = 0
                max_start_times[i] = tap.frequency_minutes

        return [list(range(min_start_time, max_start_time)) for min_start_time, max_start_time in zip(min_start_times, max_start_times)]
    

    def _initial_population(self, taps: Sequence[Tap], gene_space: List[List[int]]) -> np.ndarray:
        rng = np.random.default_rng(self.cfg.random_seed)
        seed = []

        # Add current start minutes as first solution to bias solution space towards current solution
        for i, tap in enumerate(taps):
            gs = gene_space[i]
            s = int(tap.start_time_mins)
            seed.append(max(min(s, gs[-1]), gs[0]))
        pop = [seed]

        # Populate the rest of the initial population randomly within the gene space limits for each tap
        for _ in range(self.cfg.sol_per_pop - 1):
            pop.append([int(rng.integers(0, len(gene_space[i]))) for i in range(len(taps))])
        return np.asarray(pop, dtype=int)
    
    def _blacklist(self):
        self.cfg.blacklist_schedule_ids = set(self.cfg.blacklist_schedule_ids)
        raise NotImplementedError("Blacklist functionality not yet implemented")

    def fitness_fn(self, ga, solution, solution_idx):
        _, peak = evaluate_cpu_usage_and_peak(solution, self.taps)
        return -float(peak)
        
    def solve(self, taps: Sequence[Tap]) -> tuple[Sequence[Tap], List[int], float, np.ndarray]:
        self.taps = taps
        gene_space = self._gene_space(taps)
        print("Successfully initialised gene space")
        
        initial_population = self._initial_population(taps, gene_space)
        print("Created initial population. Current Solution Start Times:")
        print(initial_population[0])

        initial_fitness = self.fitness_fn(None, initial_population[0], 0)
        print("Initial population fitness (max_cpu load):", -initial_fitness)

        ga = pygad.GA(
            num_generations=self.cfg.num_generations,
            sol_per_pop=self.cfg.sol_per_pop,
            num_parents_mating=self.cfg.num_parents_mating,
            num_genes=len(taps),
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
        peak_cpu = -float(best_fitness)

        print(f"Optimised for {self.cfg.num_generations} generations. Best Solution Start Times:")
        print(best_solution)
        
        usage, _ = evaluate_cpu_usage_and_peak(start_times, taps)

        # Update tap objects start_time_mins attribute based on GA solution
        for i, tap in enumerate(taps):
            if tap.start_time_mins != start_times[i]: 
                tap.shifted = True
                tap.start_time_mins = start_times[i]
            
        return taps, start_times, peak_cpu, usage, -initial_fitness