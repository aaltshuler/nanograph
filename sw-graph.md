# Star Wars Knowledge Graph

Graph database for the Star Wars universe, expressed in Datalog.

**Version:** 0.5.0
**Last Updated:** 2026-02-07

---

## Changelog

### v0.5.0 (2026-02-07)
- Added 3 characters: Grand Moff Tarkin, Jabba the Hutt, General Grievous
- Added 3 node types: Weapon (6), Battle (6), Location (5)
- Added 3 planets: Alderaan, Coruscant, Mustafar
- Added 5 relationship types: WIELDS, PARTICIPATED_IN, LOCATED_AT, CLONED_FROM, TOOK_PLACE_ON
- Replaced `content` property with `note` (2-3 sentence descriptions per node)
- Replaced Cypher with Datalog
- Removed Proposed Additions section (all items implemented)

### v0.4.0 (2026-02-07)
- Consolidated into single document (removed `sw-ontology.md` and `main.cypher`)
- Fixed Darth Vader missing `gender` property
- Added `rank` to Obi-Wan Kenobi, Yoda, Leia Organa
- Added Leia `AFFILIATED_WITH` Rebel Alliance, Palpatine `AFFILIATED_WITH` Empire
- Added Padme `ORIGINATES_FROM` Naboo

### v0.3.0 (2026-01-01)
- Initial stable release with `content` and `slug` fields
- Fixed Anakin/Vader separation, completed mentor chains, cleaned duplicates

### v0.2.0 (2025-12-31)
- Added Starships, prequel/Clone Wars characters, Bounty Hunters Guild, Separatists
- Added `era` property, BECOMES, FOUGHT, OWNS, RULES, HIRED_BY, CAPTURED

### v0.1.0 (2025-12-31)
- Initial graph with Original Trilogy characters, core factions, and planets

---

## Schema Overview

### Node Counts
| Label | Count |
|-------|-------|
| Character | 20 (11 Heroes, 9 Villains) |
| Droid | 2 |
| Faction | 6 |
| Planet | 8 |
| Starship | 6 |
| Weapon | 6 |
| Battle | 6 |
| Location | 5 |
| **Total** | **59** |

### Relationship Counts
| Relationship | Count |
|--------------|-------|
| PARTICIPATED_IN | 31 |
| AFFILIATED_WITH | 22 |
| HAS_MENTOR | 11 |
| FOUGHT | 10 |
| WIELDS | 7 |
| ORIGINATES_FROM | 7 |
| OWNS | 7 |
| HAS_PARENT | 5 |
| HIRED_BY | 3 |
| LOCATED_AT | 3 |
| MARRIED_TO | 2 |
| LOCATED_IN | 2 |
| RULES | 2 |
| BECOMES | 1 |
| BUILT_BY | 1 |
| CAPTURED | 1 |
| CLONED_FROM | 1 |
| HAS_COMPANION | 1 |
| HAS_CO_PILOT | 1 |
| SIBLING_OF | 1 |
| TOOK_PLACE_ON | 1 |
| **Total** | **120** |

---

## Node Labels

### Character
Base label for all sentient beings. Combined with `Hero` or `Villain` sublabels.

| Property | Type | Description |
|----------|------|-------------|
| name | STRING | Character's full name |
| note | STRING | 2-3 sentence description |
| slug | STRING | URL-safe identifier (derived from name) |
| species | STRING | Species (Human, Togruta, Zabrak, etc.) |
| gender | STRING | Male/Female |
| rank | STRING | Title or rank (optional, `none` if absent) |
| era | STRING | Primary era (Prequel, Clone Wars, Original Trilogy, Empire) |

**Sublabels:** `Hero` (light side), `Villain` (dark side / antagonist)

### Droid
| Property | Type | Description |
|----------|------|-------------|
| name | STRING | Droid designation |
| note | STRING | 2-3 sentence description |
| slug | STRING | Derived from name |
| model | STRING | Droid model type |

### Planet
| Property | Type | Description |
|----------|------|-------------|
| name | STRING | Planet name |
| note | STRING | 2-3 sentence description |
| slug | STRING | Derived from name |
| terrain | STRING | Primary terrain type |

### Faction
| Property | Type | Description |
|----------|------|-------------|
| name | STRING | Faction name |
| note | STRING | 2-3 sentence description |
| slug | STRING | Derived from name |

### Starship
| Property | Type | Description |
|----------|------|-------------|
| name | STRING | Ship name or designation |
| note | STRING | 2-3 sentence description |
| slug | STRING | Derived from name |
| model | STRING | Model number |
| class | STRING | Ship class (Freighter, Starfighter, Yacht, Patrol Craft) |
| manufacturer | STRING | Manufacturing company |

### Weapon
| Property | Type | Description |
|----------|------|-------------|
| name | STRING | Weapon name |
| note | STRING | 2-3 sentence description |
| slug | STRING | Derived from name |
| type | STRING | Weapon type (Lightsaber, Blaster, Bowcaster) |
| color | STRING | Blade/bolt color |

### Battle
| Property | Type | Description |
|----------|------|-------------|
| name | STRING | Battle or event name |
| note | STRING | 2-3 sentence description |
| slug | STRING | Derived from name |
| era | STRING | Era (Prequel, Clone Wars, Original Trilogy) |
| outcome | STRING | Result of the engagement |

### Location
| Property | Type | Description |
|----------|------|-------------|
| name | STRING | Location name |
| note | STRING | 2-3 sentence description |
| slug | STRING | Derived from name |
| type | STRING | Location type (Space Station, City, Temple, Cantina, Palace) |

---

## Relationships

### Family & Personal
| Relationship | From | To | Properties | Description |
|--------------|------|-----|------------|-------------|
| HAS_PARENT | Character | Character | — | Child -> parent |
| SIBLING_OF | Character | Character | — | One direction, query undirected |
| MARRIED_TO | Character | Character | — | One direction, query undirected |
| CLONED_FROM | Character | Character | — | Genetic clone -> original template |

### Training & Mentorship
| Relationship | From | To | Properties | Description |
|--------------|------|-----|------------|-------------|
| HAS_MENTOR | Character | Character | — | Student -> teacher |

### Affiliation & Ownership
| Relationship | From | To | Properties | Description |
|--------------|------|-----|------------|-------------|
| AFFILIATED_WITH | Character | Faction | — | Membership in organization |
| OWNS | Character/Faction | Starship | — | Ownership of vessel |
| RULES | Character | Faction/Planet | title | Leadership/governance |

### Origin & Location
| Relationship | From | To | Properties | Description |
|--------------|------|-----|------------|-------------|
| ORIGINATES_FROM | Character/Droid | Planet | — | Birthplace/creation location |
| LOCATED_IN | Character | Planet | status | Current location |
| LOCATED_AT | Location | Planet | — | Sub-planetary place -> parent planet |

### Conflict & Action
| Relationship | From | To | Properties | Description |
|--------------|------|-----|------------|-------------|
| FOUGHT | Character | Character | location | Combat encounter |
| CAPTURED | Character | Character | — | Capture/imprisonment |
| HIRED_BY | Character | Character | purpose | Employment/contract |
| PARTICIPATED_IN | Character | Battle | role | Participation in battle event |
| TOOK_PLACE_ON | Battle | Planet | — | Where a battle occurred |

### Transformation
| Relationship | From | To | Properties | Description |
|--------------|------|-----|------------|-------------|
| BECOMES | Character | Character | event | Identity transformation |

### Droid, Companion & Equipment
| Relationship | From | To | Properties | Description |
|--------------|------|-----|------------|-------------|
| BUILT_BY | Droid | Character | — | Creator of droid |
| HAS_COMPANION | Droid | Droid | — | Droid partnership |
| HAS_CO_PILOT | Character | Character | — | Co-pilot relationship |
| WIELDS | Character | Weapon | — | Weapon wielded by character |

### Conventions

**Symmetric relationships** (`SIBLING_OF`, `MARRIED_TO`) are stored as a single directed edge. Always query undirected:
```datalog
%% Correct — matches both directions
family(A, B) :- sibling_of(A, B).
family(A, B) :- sibling_of(B, A).
```

**Optional properties** use the atom `none` when absent (e.g., `rank`, `location`).

**Slug** is derived, not stored. Rule: `slug(Id, S) :- name(Id, N), to_slug(N, S).`

---

## Example Queries

### Find all Jedi
```datalog
jedi(Name) :-
    character(Id, Name, _, _, _, _, _),
    affiliated_with(Id, jedi_order).
```

### Find mentor chain (recursive)
```datalog
mentor_chain(S, T) :- has_mentor(S, T).
mentor_chain(S, T) :- has_mentor(S, M), mentor_chain(M, T).

?- mentor_chain(ahsoka, Mentor), character(Mentor, Name, _, _, _, _, _).
```

### Find who fought whom
```datalog
combatants(AName, BName, Loc) :-
    fought(A, B, Loc),
    character(A, AName, _, _, _, _, _),
    character(B, BName, _, _, _, _, _).
```

### Find Skywalker family tree
```datalog
family(A, B) :- has_parent(A, B).
family(A, B) :- has_parent(B, A).
family(A, B) :- sibling_of(A, B).
family(A, B) :- sibling_of(B, A).
family(A, B) :- married_to(A, B).
family(A, B) :- married_to(B, A).

skywalker_kin(Name) :-
    family(luke, R),
    character(R, Name, _, _, _, _, _).
```

### Find battle roster
```datalog
roster(BattleName, CharName, Role) :-
    battle(B, BattleName, _, _, _),
    participated_in(C, B, Role),
    character(C, CharName, _, _, _, _, _).

?- roster("Battle of Yavin", Name, Role).
```

---

## Graph Creation Script

```datalog
%% ============================================
%% Star Wars Knowledge Graph — Datalog Facts
%% ============================================
%% Slug is derived: slug(Id, S) :- name(Id, N), to_slug(N, S).
%% Optional properties use the atom 'none'.

%% ============================================
%% Factions
%% ============================================
%% faction(Id, Name, Note).
faction(jedi_order, "Jedi Order", "Ancient monastic order of Force-sensitive peacekeepers serving the Galactic Republic. Members train from childhood in combat and diplomacy, following the light side of the Force.").
faction(sith_order, "Sith Order", "Dark side Force tradition following the Rule of Two: one master, one apprentice. Driven by passion and the pursuit of absolute power over the galaxy.").
faction(rebel_alliance, "Rebel Alliance", "Underground resistance formed to oppose the Galactic Empire. United diverse species and systems to restore democracy and ultimately destroyed two Death Stars.").
faction(galactic_empire, "Galactic Empire", "Authoritarian regime that replaced the Galactic Republic after Palpatine seized power. Rules through military dominance, fear, and superweapons like the Death Star.").
faction(bounty_hunters_guild, "Bounty Hunters Guild", "Loosely organized network of mercenaries and trackers operating across the galaxy. Members accept contracts for credits and follow a code of conduct among their ranks.").
faction(separatists, "Separatists", "Confederacy of Independent Systems that seceded from the Republic under Count Dooku. Fielded a massive droid army in the Clone Wars before being dissolved after Palpatine's rise.").

%% ============================================
%% Planets
%% ============================================
%% planet(Id, Name, Terrain, Note).
planet(tatooine, "Tatooine", "Desert", "Remote Outer Rim world with twin suns, home to moisture farmers and the Hutt crime syndicate. Birthplace of Anakin Skywalker and childhood home of Luke Skywalker.").
planet(naboo, "Naboo", "Grasslands, swamps", "Lush Mid Rim world home to both humans and the amphibious Gungan species. Political birthplace of Padme Amidala and Emperor Palpatine.").
planet(corellia, "Corellia", "Urban", "Major Core World renowned for its shipyards and skilled pilots. Birthplace of Han Solo and origin of the Corellian Engineering Corporation.").
planet(kashyyyk, "Kashyyyk", "Forest", "Forested Wookiee homeworld with massive wroshyr trees towering kilometers high. Enslaved by the Empire after the Clone Wars, forcing Wookiees into labor.").
planet(dagobah, "Dagobah", "Swamp", "Remote swamp planet strong in the Force, virtually undetectable by the Empire. Yoda chose it as his hiding place during exile and later trained Luke Skywalker here.").
planet(alderaan, "Alderaan", "Grasslands, mountains", "Peaceful Core World known for its culture and anti-war stance. Leia Organa's adoptive homeworld, destroyed by the Death Star as a demonstration of Imperial power.").
planet(coruscant, "Coruscant", "Cityscape", "City-planet and galactic capital, seat of both the Republic Senate and the Jedi Temple. Became the Imperial capital after Palpatine's rise to power.").
planet(mustafar, "Mustafar", "Volcanic", "Volcanic world of lava rivers on the edge of the Outer Rim. Site of the duel between Obi-Wan and Anakin that sealed Anakin's fall to the dark side.").

%% ============================================
%% Starships
%% ============================================
%% starship(Id, Name, Model, Class, Manufacturer, Note).
starship(millennium_falcon, "Millennium Falcon", "YT-1300 Light Freighter", "Freighter", "Corellian Engineering", "Heavily modified light freighter famous for the Kessel Run. Despite its battered appearance, one of the fastest ships in the galaxy.").
starship(xwing, "X-Wing", "T-65B", "Starfighter", "Incom Corporation", "Primary Rebel starfighter with S-foil attack configuration. Luke Skywalker flew one to destroy the first Death Star at the Battle of Yavin.").
starship(tie_fighter, "TIE Fighter", "TIE/ln", "Starfighter", "Sienar Fleet Systems", "Standard Imperial starfighter deployed in overwhelming numbers, lacking shields or hyperdrive. Its twin ion engines produce the iconic screaming sound of Imperial warfare.").
starship(slave_1, "Slave I", "Firespray-31", "Patrol Craft", "Kuat Systems Engineering", "Heavily armed patrol craft used by Jango and Boba Fett. Equipped with hidden weapons, tracking systems, and prisoner cages for bounty hunting.").
starship(jedi_starfighter, "Jedi Starfighter", "Delta-7 Aethersprite", "Starfighter", "Kuat Systems Engineering", "Compact interceptor requiring an external hyperdrive ring for long-range travel. Used by Jedi Knights during the Clone Wars for reconnaissance and combat.").
starship(naboo_royal_starship, "Naboo Royal Starship", "J-type 327", "Yacht", "Theed Palace Space Vessel Engineering", "Elegant chromium-plated yacht serving as official transport for Naboo royalty. Queen Amidala escaped the Trade Federation blockade aboard this vessel.").

%% ============================================
%% Droids
%% ============================================
%% droid(Id, Name, Model, Note).
droid(c3po, "C-3PO", "Protocol Droid", "Protocol droid fluent in over six million forms of communication, built by young Anakin Skywalker on Tatooine. Anxious and cautious, he serves as translator and cultural liaison throughout the saga.").
droid(r2d2, "R2-D2", "Astromech Droid", "Resourceful astromech who served Amidala, Anakin, and Luke across decades. Brave and loyal, he carries secret messages and saves his companions with hidden tools.").

%% ============================================
%% Heroes — Prequel Era
%% ============================================
%% character(Id, Name, Species, Gender, Rank, Era, Note).
%% hero(Id). / villain(Id).
character(quigon, "Qui-Gon Jinn", "Human", "Male", "Jedi Master", "Prequel", "Maverick Jedi Master who followed the living Force over strict Council doctrine. He discovered Anakin on Tatooine and believed him to be the Chosen One.").
hero(quigon).

character(obiwan, "Obi-Wan Kenobi", "Human", "Male", "Jedi Master", "Prequel", "Devoted Jedi Master who trained both Anakin and Luke Skywalker across two generations. Went into hiding on Tatooine after Anakin's fall to watch over the infant Luke.").
hero(obiwan).

character(anakin, "Anakin Skywalker", "Human", "Male", "Jedi Knight", "Prequel", "The Chosen One, born a slave on Tatooine with the highest midi-chlorian count ever recorded. His fear of loss and Palpatine's manipulation led to his fall to the dark side.").
hero(anakin).

character(padme, "Padme Amidala", "Human", "Female", "Senator", "Prequel", "Elected Queen of Naboo at fourteen, later a Senator who opposed the rise of authoritarianism. Secretly married Anakin and died after giving birth to twins Luke and Leia.").
hero(padme).

character(mace, "Mace Windu", "Human", "Male", "Jedi Master", "Prequel", "Senior Jedi Council member and one of the Order's greatest lightsaber combatants. Wielded a distinctive purple blade and nearly defeated Palpatine before Anakin's betrayal.").
hero(mace).

character(yoda, "Yoda", "Unknown", "Male", "Jedi Grand Master", "Prequel", "Legendary Jedi Grand Master who trained Jedi for over 800 years. After the Republic fell, he lived in exile on Dagobah until training Luke Skywalker.").
hero(yoda).

%% ============================================
%% Heroes — Clone Wars Era
%% ============================================
character(ahsoka, "Ahsoka Tano", "Togruta", "Female", "Jedi Padawan", "Clone Wars", "Togruta Padawan assigned to Anakin during the Clone Wars. Left the Jedi Order after being falsely accused and later became a key figure in the early rebellion.").
hero(ahsoka).

%% ============================================
%% Heroes — Original Trilogy Era
%% ============================================
character(luke, "Luke Skywalker", "Human", "Male", "Jedi Knight", "Original Trilogy", "Son of Anakin Skywalker, raised in obscurity on Tatooine. Destroyed the Death Star, trained under Yoda, and redeemed his father from the dark side.").
hero(luke).

character(leia, "Leia Organa", "Human", "Female", "Princess", "Original Trilogy", "Daughter of Anakin and Padme, adopted by Bail Organa of Alderaan. Fearless Rebel leader who became a diplomat, general, and symbol of resistance against the Empire.").
hero(leia).

character(han, "Han Solo", "Human", "Male", "General", "Original Trilogy", "Corellian smuggler turned Rebel hero who co-piloted the Millennium Falcon with Chewbacca. Initially motivated by money, he became one of the Alliance's greatest generals.").
hero(han).

character(chewie, "Chewbacca", "Wookiee", "Male", none, "Original Trilogy", "Wookiee warrior from Kashyyyk and Han Solo's lifelong co-pilot and partner. Fought in the Clone Wars alongside Yoda before joining the Rebellion.").
hero(chewie).

%% ============================================
%% Villains
%% ============================================
character(palpatine, "Emperor Palpatine", "Human", "Male", "Emperor", "Original Trilogy", "Sith Lord who orchestrated the Republic's fall by manipulating both sides of the Clone Wars. Ruled as Emperor through fear and the dark side until defeated at the Battle of Endor.").
villain(palpatine).

character(vader, "Darth Vader", "Human", "Male", "Sith Lord", "Original Trilogy", "The fallen Anakin Skywalker, encased in a life-support suit after his defeat on Mustafar. Served as Palpatine's enforcer until redeemed by his son Luke.").
villain(vader).

character(dooku, "Count Dooku", "Human", "Male", "Sith Lord", "Prequel", "Former Jedi Master who fell to the dark side under Palpatine's influence. Led the Separatists as a figurehead while secretly serving as Darth Tyranus.").
villain(dooku).

character(maul, "Darth Maul", "Zabrak", "Male", "Sith Apprentice", "Prequel", "Zabrak Sith apprentice trained as a weapon of pure aggression. Wielded a double-bladed red lightsaber and killed Qui-Gon Jinn on Naboo.").
villain(maul).

character(jango, "Jango Fett", "Human", "Male", "Bounty Hunter", "Prequel", "Mandalorian bounty hunter chosen as the genetic template for the Republic's clone army. Raised one unaltered clone as his son Boba before being killed by Mace Windu.").
villain(jango).

character(boba, "Boba Fett", "Human", "Male", "Bounty Hunter", "Empire", "Unaltered clone of Jango Fett, driven by vengeance after his father's death. Became the galaxy's most feared bounty hunter and captured Han Solo for Jabba.").
villain(boba).

character(tarkin, "Grand Moff Tarkin", "Human", "Male", "Grand Moff", "Original Trilogy", "Imperial governor and architect of the Tarkin Doctrine: rule through fear of force. Commanded the Death Star, ordered Alderaan's destruction, and died when it was destroyed at Yavin.").
villain(tarkin).

character(jabba, "Jabba the Hutt", "Hutt", "Male", "Crime Lord", "Original Trilogy", "Powerful crime lord controlling a vast criminal empire from his palace on Tatooine. Placed a bounty on Han Solo and kept prisoners as entertainment in his throne room.").
villain(jabba).

character(grievous, "General Grievous", "Kaleesh", "Male", "General", "Prequel", "Cybernetically enhanced warlord and Supreme Commander of the Separatist droid army. Collected lightsabers from fallen Jedi and was destroyed by Obi-Wan on Utapau.").
villain(grievous).

%% ============================================
%% Weapons
%% ============================================
%% weapon(Id, Name, Type, Color, Note).
weapon(anakins_saber, "Anakin's Lightsaber", "Lightsaber", "Blue", "Built by Anakin during the Clone Wars, later passed to Luke by Obi-Wan Kenobi. One of the most storied weapons in galactic history.").
weapon(lukes_green_saber, "Luke's Green Lightsaber", "Lightsaber", "Green", "Constructed by Luke after losing his father's weapon on Cloud City. Its creation marked Luke's transition from apprentice to Jedi Knight.").
weapon(maces_saber, "Mace Windu's Lightsaber", "Lightsaber", "Purple", "Purple blade reflecting Windu's mastery of Vaapad, a style that channels darkness without succumbing to it. One of the rarest blade colors in the Jedi Order.").
weapon(mauls_saber, "Darth Maul's Lightsaber", "Lightsaber", "Red", "Double-bladed weapon that could split into two separate sabers. Its aggressive design embodied Maul's role as an instrument of Sith violence.").
weapon(yodas_saber, "Yoda's Lightsaber", "Lightsaber", "Green", "Compact blade scaled to Yoda's small stature. Wielded with extraordinary speed and precision by the greatest Jedi Master of his era.").
weapon(vaders_saber, "Darth Vader's Lightsaber", "Lightsaber", "Red", "Built after Anakin's fall, powered by a kyber crystal bled through the dark side. Replaced the blue blade he carried as a Jedi.").

%% ============================================
%% Battles
%% ============================================
%% battle(Id, Name, Era, Outcome, Note).
battle(battle_of_naboo, "Battle of Naboo", "Prequel", "Naboo liberated", "Trade Federation invasion that ended with the Gungan diversion and Anakin's destruction of the droid control ship. Qui-Gon Jinn was killed by Darth Maul during this battle.").
battle(battle_of_yavin, "Battle of Yavin", "Original Trilogy", "Death Star destroyed", "The Rebellion's first major victory, where Luke destroyed the Death Star with a Force-guided torpedo. Proved the Empire was vulnerable and galvanized resistance across the galaxy.").
battle(battle_of_hoth, "Battle of Hoth", "Original Trilogy", "Empire victory, Rebels evacuated", "Imperial assault on the Rebel base using AT-AT walkers. The Rebels suffered heavy losses but evacuated leadership and key personnel.").
battle(battle_of_endor, "Battle of Endor", "Original Trilogy", "Rebel victory, Emperor killed", "Decisive engagement where the Rebels destroyed the second Death Star while a ground team disabled its shield. Vader turned against the Emperor, fulfilling the Chosen One prophecy.").
battle(battle_of_geonosis, "Battle of Geonosis", "Prequel", "Republic victory, Clone Wars begin", "First battle of the Clone Wars, where Jedi and clone troopers fought Separatist droid forces. Marked the start of a galaxy-wide conflict engineered by Palpatine.").
battle(order_66, "Order 66", "Prequel", "Jedi Order destroyed", "Secret protocol triggering clone troopers to execute their Jedi commanders across the galaxy. Activated by Palpatine to destroy the Jedi Order in a single devastating stroke.").

%% ============================================
%% Locations
%% ============================================
%% location(Id, Name, Type, Note).
location(death_star, "Death Star", "Space Station", "Moon-sized Imperial battle station with a planet-destroying superlaser. Two were built; both were destroyed by the Rebel Alliance at Yavin and Endor.").
location(cloud_city, "Cloud City", "City", "Tibanna gas mining colony floating in Bespin's atmosphere. Luke confronted Vader here and learned the truth about his father.").
location(jedi_temple, "Jedi Temple", "Temple", "Ancient seat of the Jedi Order on Coruscant, housing training halls, archives, and the Council chamber. Attacked during Order 66 and later repurposed as the Imperial Palace.").
location(mos_eisley_cantina, "Mos Eisley Cantina", "Cantina", "Notorious spaceport bar on Tatooine frequented by smugglers, bounty hunters, and travelers. Where Luke and Obi-Wan first met Han Solo and Chewbacca.").
location(jabbas_palace, "Jabba's Palace", "Palace", "Hutt crime lord's fortress in the Tatooine desert, built from a former B'omarr monastery. Headquarters of Jabba's criminal empire and site of Han Solo's captivity in carbonite.").

%% ============================================
%% ORIGINATES_FROM
%% ============================================
%% originates_from(EntityId, PlanetId).
originates_from(luke, tatooine).
originates_from(anakin, tatooine).
originates_from(c3po, tatooine).
originates_from(han, corellia).
originates_from(chewie, kashyyyk).
originates_from(r2d2, naboo).
originates_from(padme, naboo).

%% ============================================
%% LOCATED_IN
%% ============================================
%% located_in(CharId, PlanetId, Status).
located_in(yoda, dagobah, "Exile").
located_in(jabba, tatooine, "Resident").

%% ============================================
%% LOCATED_AT
%% ============================================
%% located_at(LocationId, PlanetId).
located_at(jedi_temple, coruscant).
located_at(mos_eisley_cantina, tatooine).
located_at(jabbas_palace, tatooine).

%% ============================================
%% Family: HAS_PARENT, SIBLING_OF, MARRIED_TO
%% ============================================
%% has_parent(ChildId, ParentId).
has_parent(luke, anakin).
has_parent(luke, padme).
has_parent(leia, anakin).
has_parent(leia, padme).
has_parent(boba, jango).

%% sibling_of(A, B). — query undirected
sibling_of(luke, leia).

%% married_to(A, B). — query undirected
married_to(anakin, padme).
married_to(han, leia).

%% ============================================
%% CLONED_FROM
%% ============================================
%% cloned_from(CloneId, OriginalId).
cloned_from(boba, jango).

%% ============================================
%% BUILT_BY
%% ============================================
%% built_by(DroidId, CharId).
built_by(c3po, anakin).

%% ============================================
%% HAS_COMPANION / HAS_CO_PILOT
%% ============================================
has_companion(c3po, r2d2).
has_co_pilot(han, chewie).

%% ============================================
%% HAS_MENTOR (Training Chains)
%% ============================================
%% has_mentor(StudentId, TeacherId).
%% Jedi lineage: Yoda -> Dooku -> Qui-Gon -> Obi-Wan -> Anakin -> Ahsoka
has_mentor(dooku, yoda).
has_mentor(quigon, dooku).
has_mentor(obiwan, quigon).
has_mentor(anakin, obiwan).
has_mentor(ahsoka, anakin).
has_mentor(ahsoka, obiwan).
%% Luke's mentors
has_mentor(luke, obiwan).
has_mentor(luke, yoda).
%% Sith lineage
has_mentor(maul, palpatine).
has_mentor(vader, palpatine).
%% Separatist
has_mentor(grievous, dooku).

%% ============================================
%% AFFILIATED_WITH
%% ============================================
%% affiliated_with(CharId, FactionId).
%% Jedi Order
affiliated_with(quigon, jedi_order).
affiliated_with(obiwan, jedi_order).
affiliated_with(anakin, jedi_order).
affiliated_with(mace, jedi_order).
affiliated_with(yoda, jedi_order).
affiliated_with(ahsoka, jedi_order).
affiliated_with(luke, jedi_order).
%% Rebel Alliance
affiliated_with(luke, rebel_alliance).
affiliated_with(han, rebel_alliance).
affiliated_with(chewie, rebel_alliance).
affiliated_with(leia, rebel_alliance).
%% Sith Order
affiliated_with(palpatine, sith_order).
affiliated_with(vader, sith_order).
affiliated_with(dooku, sith_order).
affiliated_with(maul, sith_order).
%% Galactic Empire
affiliated_with(palpatine, galactic_empire).
affiliated_with(vader, galactic_empire).
affiliated_with(tarkin, galactic_empire).
%% Separatists
affiliated_with(dooku, separatists).
affiliated_with(grievous, separatists).
%% Bounty Hunters Guild
affiliated_with(jango, bounty_hunters_guild).
affiliated_with(boba, bounty_hunters_guild).

%% ============================================
%% OWNS
%% ============================================
%% owns(OwnerId, StarshipId).
owns(han, millennium_falcon).
owns(luke, xwing).
owns(obiwan, jedi_starfighter).
owns(padme, naboo_royal_starship).
owns(jango, slave_1).
owns(boba, slave_1).
owns(galactic_empire, tie_fighter).

%% ============================================
%% RULES
%% ============================================
%% rules(CharId, TargetId, Title).
rules(palpatine, galactic_empire, "Emperor").
rules(padme, naboo, "Queen").

%% ============================================
%% BECOMES (Identity Transformation)
%% ============================================
%% becomes(FromId, ToId, Event).
becomes(anakin, vader, "Fall to the Dark Side").

%% ============================================
%% FOUGHT
%% ============================================
%% fought(AttackerId, DefenderId, Location).
fought(obiwan, anakin, "Mustafar").
fought(obiwan, maul, "Naboo").
fought(luke, vader, "Cloud City").
fought(yoda, dooku, "Geonosis").
fought(yoda, palpatine, "Coruscant").
fought(mace, palpatine, "Coruscant").
fought(mace, jango, "Geonosis").
fought(anakin, dooku, "Coruscant").
fought(jango, obiwan, "Kamino").
fought(grievous, obiwan, "Utapau").

%% ============================================
%% HIRED_BY
%% ============================================
%% hired_by(CharId, EmployerId, Purpose).
hired_by(jango, dooku, "Clone template").
hired_by(boba, vader, "Capture Han Solo").
hired_by(boba, jabba, "Bounty on Han Solo").

%% ============================================
%% CAPTURED
%% ============================================
%% captured(CaptorId, CapturedId).
captured(boba, han).

%% ============================================
%% WIELDS
%% ============================================
%% wields(CharId, WeaponId).
wields(anakin, anakins_saber).
wields(luke, anakins_saber).
wields(luke, lukes_green_saber).
wields(mace, maces_saber).
wields(maul, mauls_saber).
wields(yoda, yodas_saber).
wields(vader, vaders_saber).

%% ============================================
%% PARTICIPATED_IN
%% ============================================
%% participated_in(CharId, BattleId, Role).
%% Battle of Naboo
participated_in(quigon, battle_of_naboo, "Jedi combatant").
participated_in(obiwan, battle_of_naboo, "Jedi combatant").
participated_in(maul, battle_of_naboo, "Sith combatant").
participated_in(padme, battle_of_naboo, "Palace assault leader").
participated_in(anakin, battle_of_naboo, "Starfighter pilot").
%% Battle of Yavin
participated_in(luke, battle_of_yavin, "X-Wing pilot").
participated_in(han, battle_of_yavin, "Millennium Falcon pilot").
participated_in(vader, battle_of_yavin, "TIE fighter pilot").
participated_in(tarkin, battle_of_yavin, "Death Star commander").
%% Battle of Hoth
participated_in(luke, battle_of_hoth, "Snowspeeder pilot").
participated_in(han, battle_of_hoth, "Evacuation leader").
participated_in(leia, battle_of_hoth, "Base commander").
participated_in(chewie, battle_of_hoth, "Evacuation support").
participated_in(vader, battle_of_hoth, "Assault commander").
%% Battle of Endor
participated_in(luke, battle_of_endor, "Jedi combatant").
participated_in(han, battle_of_endor, "Ground assault leader").
participated_in(leia, battle_of_endor, "Ground assault team").
participated_in(chewie, battle_of_endor, "Ground assault team").
participated_in(vader, battle_of_endor, "Imperial enforcer").
participated_in(palpatine, battle_of_endor, "Emperor").
%% Battle of Geonosis
participated_in(obiwan, battle_of_geonosis, "Captured, then combatant").
participated_in(anakin, battle_of_geonosis, "Rescue team, then combatant").
participated_in(padme, battle_of_geonosis, "Rescue team").
participated_in(mace, battle_of_geonosis, "Jedi strike team leader").
participated_in(yoda, battle_of_geonosis, "Clone army commander").
participated_in(dooku, battle_of_geonosis, "Separatist leader").
participated_in(jango, battle_of_geonosis, "Separatist combatant").
%% Order 66
participated_in(palpatine, order_66, "Issued the order").
participated_in(yoda, order_66, "Survivor").
participated_in(obiwan, order_66, "Survivor").
participated_in(ahsoka, order_66, "Survivor").

%% ============================================
%% TOOK_PLACE_ON
%% ============================================
%% took_place_on(BattleId, PlanetId).
took_place_on(battle_of_naboo, naboo).
```
