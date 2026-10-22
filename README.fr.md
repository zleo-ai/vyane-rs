# Vyane

**Exécutez des agents de programmation et des API de modèles avec une seule CLI en Rust.**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

Pour les développeurs qui utilisent plusieurs agents de programmation ou fournisseurs de modèles : nommez vos cibles, envoyez une tâche à une ou plusieurs d’entre elles et conservez ensemble les résultats et l’historique des tentatives. Utilisez Claude Code ou Codex CLI pour programmer, ou appelez directement une API de modèles pour obtenir du texte.

> **Développement initial.** Installation depuis les sources uniquement ; les API sont instables et certaines capacités restent partielles ou absentes. La reprise des sessions natives et la réexécution automatique après redémarrage ne sont pas terminées. Consultez les [limites actuelles](docs/PROJECT-STATUS.md).

## Voir le fonctionnement

[Lancez la démonstration dans le terminal](docs/EXAMPLES.md) : la CLI installée exécute une tâche, l’envoie à deux cibles en parallèle, puis se rétablit après une panne programmée de la cible principale. Elle utilise des réponses HTTP de test locales, sans clé ni appel à un véritable modèle.

```text
Local HTTP fixtures only; no live model calls.
dispatch: fixture: chat-model; recorded attempts=1
broadcast: 2 separate successful results (chat-model, review-model)
failover: fixture: review-model; recorded attempts=2
```

## Trois façons de l’utiliser

| Pour… | Utilisez | Résultat |
| --- | --- | --- |
| Exécuter une tâche avec l’agent ou le modèle choisi | [`dispatch --target chat`](docs/EXAMPLES.md#one-task-one-target) | Un résultat et un enregistrement local. Les adaptateurs d’agents nécessitent une installation et une configuration séparées. |
| Comparer les réponses au même prompt | [`broadcast --targets chat,review`](docs/EXAMPLES.md#one-prompt-several-targets) | Des exécutions simultanées avec des résultats distincts ; vous évaluez les réponses. |
| Essayer une cible de secours configurée après un échec | [`dispatch --target resilient`](docs/EXAMPLES.md#an-explicit-backup) | Une chaîne de secours ordonnée pour les erreurs admissibles, avec chaque tentative enregistrée. Ce n’est pas un transfert de session. |

## Démarrage rapide

**1. Installez depuis les sources.** Rust/Cargo stable (Rust 1.88 minimum) et des outils de compilation C/C++ sont nécessaires. La CI couvre Linux et macOS ; la prise en charge de Windows n’est pas revendiquée.

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
```

Version attendue : `vyane 0.1.0`. Le binaire reste dans cette copie du dépôt et ne remplace pas un autre `vyane` dans votre PATH. Gardez ce shell ouvert.

**2. Obtenez un premier résultat sans clé.** L’exemple fourni prévisualise uniquement le routage ; il n’envoie aucune requête à un modèle.

```sh
"$VYANE" --config docs/examples/offline.toml check
"$VYANE" --config docs/examples/offline.toml route "Explain this function" --json
```

Repérez le profil `offline` et `"selection_key": "offline"` dans le JSON. L’endpoint est volontairement invalide ; ne lui envoyez pas de tâche.

**3. Connectez votre propre cible.** Suivez la [configuration du fournisseur](docs/QUICKSTART.md#configure-a-real-target), puis essayez les [trois exemples](docs/EXAMPLES.md). Vous fournissez l’accès au modèle et les identifiants ; les appels réels peuvent être facturés par le fournisseur. L’aperçu hors ligne ne valide pas un fournisseur réel.

## Ce qui fonctionne aujourd’hui

| Disponible dans les sources | Limite |
| --- | --- |
| Exécution individuelle, diffusion parallèle, bascule explicite et streaming | Le HTTP direct renvoie le texte du modèle ; il ne joint pas de fichiers locaux et ne fournit pas d’outils shell. |
| Adaptateurs Claude Code et Codex CLI | Sous-processus CLI, sans Codex App Server RPC ni intégration directe de Claude Agent SDK. Vous fournissez la CLI et l’accès ; l’environnement d’exécution applique son sandbox. |
| Routage par profils et historique local | Le routage suit votre configuration ; il ne garantit pas la qualité des réponses. |
| Entrées CLI, REST et MCP | Chacune expose un sous-ensemble différent. Consultez le [guide CLI](docs/CLI-GUIDE.md). |

L’exécution résidente, les workflows persistants et les objectifs locaux évoluent encore. Les exécutions natives repartent de zéro, sans session ; la reprise des approbations et sessions natives ainsi que la réexécution automatique après redémarrage restent à réaliser. Les champs de propriétaire et l’authentification loopback n’isolent pas les accès hostiles d’un même utilisateur ou de plusieurs utilisateurs. Consultez l’[état et les limites](docs/PROJECT-STATUS.md) avant de construire sur ces fonctions.

## Pour approfondir

- [Architecture et modèle des cibles](docs/ARCHITECTURE.md) · [Référence de configuration](profiles.example.toml) · [Guide CLI, REST et MCP](docs/CLI-GUIDE.md)
- [Matrice des capacités et preuves reproductibles](docs/PROJECT-STATUS.md#relationship-to-python-vyane) · [Feuille de route](docs/ROADMAP.md)
- [Journal des modifications](docs/CHANGELOG.md) · [Politique de versions et de publication](docs/RELEASING.md)

<details>
<summary>État des preuves</summary>

La [matrice publique des capacités](docs/parity/ORIGINAL-VYANE-PARITY.md) suit 53 capacités dans 8 domaines : 7 implémentées, 23 partielles, 12 absentes, 9 volontairement différentes ou en attente de décision et 2 planifiées. Le [manifeste expurgé](docs/parity/fixtures/v1/manifest.json) contient 44 cas dans 5 suites : 30 correspondances après normalisation et 14 différences ouvertes. Ces vérifications limitées ne représentent pas un pourcentage de parité du système entier.

</details>

Ce dépôt est l’implémentation publique en Rust de capacités du système privé Python Vyane d’origine. Il ne le remplace pas directement : commandes, configuration, sessions enregistrées et workflows nécessitent chacun une vérification de migration. Ne partagez pas les répertoires d’état sans procédure documentée. Les contributeurs publics n’ont pas besoin du code privé.

## Contribuer et demander de l’aide

Essayez un exemple, signalez un problème reproductible, améliorez une traduction ou comblez une lacune documentée. Consultez les [Issues](https://github.com/zleo-ai/vyane-rs/issues) et les [PR ouvertes](https://github.com/zleo-ai/vyane-rs/pulls), puis lisez [CONTRIBUTING.md](CONTRIBUTING.md). Indiquez la révision du code et une sortie expurgée ; omettez les clés, prompts privés et chemins personnels. Signalez les vulnérabilités selon [SECURITY.md](SECURITY.md).

## Licence

[MIT](LICENSE-MIT) ou [Apache-2.0](LICENSE-APACHE), au choix. Les contributions suivent les mêmes conditions.

---

Base du code : `fe43941` · Workspace : `0.1.0` · Mise à jour : 2026-10-15. L’anglais fait foi ; cette traduction assistée par machine attend une relecture humaine par des locuteurs natifs. Voir la [couverture et l’état des traductions](docs/TRANSLATING.md). Les jalons de la feuille de route ne sont pas des versions publiées.
