image-mirror
============

to run sentry development we have ~several docker registries -- docker hub
being the flakiest.

this mirrors images from docker hub into github's registry (ghcr) so we have
one fewer place to pull from

Adding images
-------------

Add to the array in `main.py`, then run `python3 main.py update`.
