# CLAUDE.md

@AGENTS.md

## Claude Code specifics

- **Skills** in `.claude/skills/`:
  - `port-lab`: rebuild the current lesson's lab in another language, following the lesson contract.
  - `run-experiment`: run the lesson's load-test sequence and report what was measured.
- **Agent** in `.claude/agents/`:
  - `lab-reviewer`: reviews a port or a change against the lesson contract, with measurement integrity first. Use it before calling a port done.
- **Hook:** at session start, `.claude/settings.json` prints the current branch and the matching lesson contract, so you always know which lesson you are working on.

When the user asks to "build this in Java" (or any language), use the `port-lab` skill, then the `lab-reviewer` agent, then the `run-experiment` skill against the new port.
