# Instruksjoner for Claude

## Pull requests og merging

- Claude kan merge sine egne pull requests i dette repoet uten å spørre først, når
  - CI er grønn på PR-ens siste commit,
  - det ikke er merge-konflikter, og
  - det ikke er åpne review-kommentarer som må følges opp.
- Bruk squash-merge.
- Er CI rød: finn og fiks årsaken først. Ikke hopp over, slå av eller sett tester i karantene for å få grønt.
