


# Star Wars Knowledge Graph

A typed property graph of the Star Wars universe — 59 nodes, 120 edges, 22 queries.

## What's in here

The graph covers the prequel and original trilogies.

```
                    |*                 ___
    .---.           |.--..            /() \
   /|_:_|\         .'_____'.        _|_____|_
  / (_Y_) \        |~xxxxx~|       | | === | |
 (  \/M\/  )       |_  #  _|       |_|  O  |_|
  '.\   /.'      .---`-#-'---.      || _*_ ||
    '---'       (___\_____/_.`)    |~ \___/ ~|
```

**20 characters** — 11 heroes (Luke, Obi-Wan, Yoda, Ahsoka, Han Solo, etc.) and 9 villains (Palpatine, Vader, Maul, Boba Fett, Grievous, etc.). Each has species, era, rank, and alignment.

**6 factions** — Jedi Order, Sith Order, Rebel Alliance, Galactic Empire, Bounty Hunters Guild, Separatists. Characters can belong to multiple factions (Palpatine is both Sith and Empire).

**8 planets** — Tatooine, Naboo, Corellia, Kashyyyk, Dagobah, Alderaan, Coruscant, Mustafar. Characters have origin planets and current locations.

**6 starships** — Millennium Falcon, X-Wing, TIE Fighter, Slave I, Jedi Starfighter, Naboo Royal Starship. Owned by characters or factions.

**6 weapons** — Lightsabers with blade colors (Anakin's blue, Mace's purple, Maul's red double-blade, Yoda's green).

**6 battles** — Naboo, Geonosis, Yavin, Hoth, Endor, Order 66. Characters participated with specific roles.

**5 locations** — Death Star, Cloud City, Jedi Temple, Mos Eisley Cantina, Jabba's Palace. Each on a planet.

## Relationships

- **Training lineage**: Yoda → Dooku → Qui-Gon → Obi-Wan → Anakin → Ahsoka. Multi-hop queries walk this chain.
- **Family**: Anakin and Padme are parents of Luke and Leia. Boba is a clone of Jango.
- **Duels**: Obi-Wan vs Anakin on Mustafar, Luke vs Vader on Cloud City, Mace vs Jango on Geonosis.
- **Transformation**: Anakin *becomes* Darth Vader.
- **Contracts**: Jango hired by Dooku as clone template. Boba hired by Vader and Jabba to capture Han.

## Files

| File | What |
|------|------|
| `starwars.pg` | Schema — 8 node types, 23 edge types |
| `starwars.jsonl` | Data — 59 nodes, 120 edges |
| `starwars.gq` | 22 queries |

## Run it

```bash
nanograph init sw.nanograph --schema examples/starwars/starwars.pg
nanograph load sw.nanograph --data examples/starwars/starwars.jsonl
nanograph check --db sw.nanograph --query examples/starwars/starwars.gq
nanograph run --db sw.nanograph --query examples/starwars/starwars.gq --name jedi
```

## Sample queries

**Find all Jedi:**
```
query jedi() {
    match {
        $c: Character
        $c affiliatedWith $f
        $f.name = "Jedi Order"
    }
    return { $c.name, $c.rank, $c.era }
    order { $c.name asc }
}
```

**Two-hop training lineage:**
```
query yoda_grand_students() {
    match {
        $y: Character { name: "Yoda" }
        $s hasMentor $y
        $gs hasMentor $s
    }
    return { $s.name, $gs.name }
}
```

**Parameterized query:**
```
query faction_members($faction: String) {
    match {
        $f: Faction { name: $faction }
        $c affiliatedWith $f
    }
    return { $c.name, $c.rank }
}
```
```bash
nanograph run --db sw.nanograph --query starwars.gq --name faction_members --param faction="Sith Order"
```

**Aggregation — most battles fought:**
```
query battle_veterans() {
    match {
        $c: Character
        $c participatedIn $b
    }
    return { $c.name, count($b) as battles }
    order { battles desc }
    limit 5
}
```

**Negation — characters without a faction:**
```
query unaffiliated() {
    match {
        $c: Character
        not { $c affiliatedWith $_ }
    }
    return { $c.name }
}
```

See `starwars.gq` for all 22 queries.
