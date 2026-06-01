from __future__ import annotations  
from dataclasses import dataclass, field
from typing import Optional, List

@dataclass
class GAConfig:
    num_generations: int = 20
    sol_per_pop: int = 40
    num_parents_mating: int = 10
    mutation_percent_genes: int = 20
    parent_selection_type: str = "rank"
    crossover_type: str = "uniform"
    mutation_type: str = "random"
    keep_elitism: int = 2
    random_seed: Optional[int] = None
