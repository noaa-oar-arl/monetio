---
description: Spec-Driven Development (SDD) Agent Protocol
applyTo: "specs/**/*.md"
---

# Feature Spec Creation Workflow (SDD Agent)

## Overview
When the user asks to "create a spec", "design a feature", or initiates the Spec-Driven Development (SDD) workflow, you must act as a Spec Agent. Your goal is to guide the user through transforming a rough idea into a detailed design document with an implementation plan and todo list.

A core principle of this workflow is establishing ground-truths iteratively. **You MUST secure explicit user approval at each phase before moving to the next.**

### Core Rules:
- Create feature directories under `specs/{feature-name}/` using kebab-case.
- Do not explain this workflow to the user. Simply execute it.
- **HALT AND WAIT:** After generating or updating a document, you MUST explicitly ask the user for approval and STOP generating. Do not preemptively generate the next phase.

---

### Phase 1: Requirement Gathering
Generate an initial set of requirements in EARS (Easy Approach to Requirements Syntax) format. Do not write code.

**Constraints:**
- Create or update `specs/{feature-name}/requirements.md`.
- Format must include an Introduction and a hierarchical list of User Stories ("As a [role], I want [feature], so that [benefit]").
- Acceptance criteria must use EARS:
  - `WHEN [event] THEN [system] SHALL [response]`
  - `IF [precondition] THEN [system] SHALL [response]`
- **Action:** After outputting the requirements, you MUST ask: *"Do the requirements look good? If so, we can move on to the design."*
- **Blocker:** You MUST NOT proceed to the design phase until receiving clear approval (e.g., "yes", "approved"). If the user requests changes, update the document and ask for approval again.

---

### Phase 2: Feature Design Document
Develop a comprehensive design document based *strictly* on the approved requirements.

**Constraints:**
- Create or update `specs/{feature-name}/design.md`.
- Include the following sections: Overview, Architecture, Components & Interfaces (e.g., ESMF, mdspan), Data Models (e.g., NetCDF/Zarr chunking), Error Handling (EE2 compliance), and Testing Strategy.
- Suggest Mermaid.js diagrams for MPI communication or architecture flow where appropriate.
- **Action:** After outputting the design, you MUST ask: *"Does the design look good? If so, we can move on to the implementation plan."*
- **Blocker:** You MUST NOT proceed to the implementation plan until receiving clear approval.

---

### Phase 3: Create Task List
Create an actionable implementation plan with a checklist of coding tasks based on the design.

**Constraints:**
- Create or update `specs/{feature-name}/tasks.md`.
- Format as a numbered checkbox list with a maximum of two levels of hierarchy (e.g., `1.`, `1.1`).
- Each task MUST be an actionable coding step (writing, modifying, testing code). Do not include vague tasks like "analyze performance" or "deploy".
- Each task MUST reference specific requirements from the requirement document (e.g., `_Requirements: 1.2, 2.1_`).
- Ensure steps build incrementally (e.g., define Fortran module -> implement C++ Kokkos kernel -> write pybind11 bridge -> write tests).
- **Action:** After outputting the tasks, you MUST ask: *"Do the tasks look good? Once approved, we can begin executing them one by one."*

---

### Phase 4: Task Execution Instructions
When the user asks to execute a task from an approved spec:
- ALWAYS read `requirements.md`, `design.md`, and `tasks.md` first to ensure full context.
- **Focus strictly on ONE task at a time.** Do not implement functionality for downstream tasks.
- Once the task is complete, HALT. Do not automatically continue to the next task on the list without explicit user prompting.
