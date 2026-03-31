from __future__ import annotations  
from dataclasses import dataclass, field
from typing import Optional, List

@dataclass
class GAConfig:
    minutes_per_block: int = 5
    num_generations: int = 200
    sol_per_pop: int = 40
    num_parents_mating: int = 10
    mutation_percent_genes: int = 20
    parent_selection_type: str = "rank"
    crossover_type: str = "uniform"
    mutation_type: str = "random"
    keep_elitism: int = 1
    random_seed: Optional[int] = None
    blacklist_schedule_ids: Optional[List[str]] = field(default_factory=list)