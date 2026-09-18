"""Store unifie : dataset parquet partitionne ``layer=/field=/<annee>.parquet``.

Une seule table logique a la lecture (``read``), des petits fichiers a l'ecriture :
le daily ne reecrit que l'annee courante de chaque champ.
"""

from .layout import LAYERS  # noqa: F401
from .reader import (  # noqa: F401
    fields, first_valid, membership_mask, read, read_benchmark, read_fx, read_refdata, years,
)
