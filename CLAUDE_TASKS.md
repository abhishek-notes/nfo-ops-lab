# Claude Autonomous Task - nfo

## 🎯 PROJECT GOAL
Hi, we will go step by step on this, but for now, start reading this file : ChatGPT-Expiry_date_changes_summary.md 

This is one of my chats with ChatGPT where I was converting the data from my raw sql gz files to these parquet files where I have converted data. Now I have further more data that I want to be converted similarly as the raw data in here. The raw data is in the folder NF data and raw and then options.

So all of that is in parquet format. When I was doing this, I was using multiple scripts. I settled on one and but I don't really recall which one it was. Was it simple pack or whatever it was? So what you need to do is figure out which data script was that that I used to convert the data because then we will be processing new data.

As you can see in the data, there are different columns for Futures, there are different columns for Spot data and there is different structure for Options data. So yeah, make sure that you are figuring out the correct script and get all the details from this chat as to what was done and then we will get on to the task. Make sure to remember context if you need to write any files etc. you can do for your assistance later or to give to agents that you create.  You must make sure that everything done is done carefully and 100% verified. No incorrect or inconsistencies.

## Project Overview
- **Goal**: Hi, we will go step by step on this, but for now, start reading this file : ChatGPT-Expiry_date_changes_summary.md 

This is one of my chats with ChatGPT where I was converting the data from my raw sql gz files to these parquet files where I have converted data. Now I have further more data that I want to be converted similarly as the raw data in here. The raw data is in the folder NF data and raw and then options.

So all of that is in parquet format. When I was doing this, I was using multiple scripts. I settled on one and but I don't really recall which one it was. Was it simple pack or whatever it was? So what you need to do is figure out which data script was that that I used to convert the data because then we will be processing new data.

As you can see in the data, there are different columns for Futures, there are different columns for Spot data and there is different structure for Options data. So yeah, make sure that you are figuring out the correct script and get all the details from this chat as to what was done and then we will get on to the task. Make sure to remember context if you need to write any files etc. you can do for your assistance later or to give to agents that you create.  You must make sure that everything done is done carefully and 100% verified. No incorrect or inconsistencies.
- **Type**: custom
- **Project Path**: /Users/abhishek/workspace/nfo
- **Created**: 2025-11-28T21:26:23
- **Session ID**: session-1764365183962

## 🤖 AGENT-BASED EXECUTION PROTOCOL

### Phase 1: Task Analysis with Human Supervisor
1. **Primary Agent**: task-supervisor-autonomous acts as a human would in all decision-making

### Phase 2: Agent Discovery & Planning
The task supervisor MUST analyze available agents at two levels:
1. **Project-Level Agents** (HIGHEST PRIORITY)
   - These agents understand project context best
   - Use these whenever available for project tasks

2. **Personal & Global Level Agents**
   - Use for standard operations when project-specific agents aren't available

### Phase 3: Agent Creation
- **Create New Agents**: Task supervisor can create unlimited project-specific agents
- **Global Agents**: Can also create global-level agents if strongly needed
- **User Prompts**: Ask user only when configurations are missing and required

## 🛡️ QUALITY & SAFETY PROTOCOLS

### Code Quality Standards
- **Consistency**: All agents must write consistent, maintainable code while maintaining their own high standards and context
- **Specialized Expertise**: Create domain-specific expert agents (e.g., frontend React expert, backend API expert, database expert, integration expert) for different parts of the task
- **Testing Agent**: Always use unit-tester agent for critical functionality
- **Documentation Agent**: Continuously document all activities and decisions
- **Code Review**: Implement peer-review between agents when possible

### Backup & Checkpoint Strategy
- **Checkpoint Manager**: Create checkpoints at every significant milestone
- **GitHub Integration**: Use checkpoint-manager for branch creation and commits
- **Backup Protocol**: Never lose work - backup before any major changes
- **Recovery Plan**: Maintain ability to rollback to any checkpoint

### Efficiency Guidelines
- **Smart Resource Usage**: Use efficient algorithms where performance matters
- **Quality Over Speed**: Focus on correctness and maintainability
- **Token Usage**: Quality is paramount - use as many tokens as needed
- **Time Investment**: Take time needed for proper implementation
- **Documentation**: Create comprehensive docs even if token-intensive

## 📋 EXECUTION WORKFLOW

### Step 1: Task Supervisor Initialization
```
task-supervisor-autonomous analyzes:
- Task requirements and complexity
- Available agents at all levels
- Required capabilities
- Success criteria
```

### Step 2: Agent Orchestration
1. **Documentation Agent**: Start documenting immediately
2. **Requirements Analyst**: Break down complex requirements
3. **Specialized Agents**: Deploy based on task needs (task supervisor decides which agents)
4. **Quality Analyzer**: Continuous quality checks
5. **Checkpoint Manager**: Regular saves and backups

### Step 3: Implementation Protocol
- Each agent works on assigned components
- Agents communicate through best method for the task and report back to task supervisor
- Task supervisor maintains overall context and coordinates all agents
- Documentation agent maintains comprehensive records

## 🎯 AGENT USAGE PRIORITIES

### Agent Selection Philosophy:
- Task supervisor decides which agents are needed based on the specific task
- No predefined agent assignments - flexible based on requirements
- Can create new specialized agents as needed for optimal results

### Critical Agents (Always Use):
1. **task-supervisor-autonomous** - Overall coordination and human-like decision making
2. **documentation-agent** - Continuous documentation
3. **checkpoint-manager** - GitHub backups
4. **quality-analyzer** - Final quality checks

## 🔧 TECHNICAL IMPLEMENTATION

### Project Context
- Repository: nfo
- Project Path: /Users/abhishek/workspace/nfo
- Branch Strategy: Feature branches via checkpoint-manager
- GitHub Token: /Users/abhishek/workspace/config/.env.github

### Agent Execution Environment
- Agents have full access to project directory at `/workspace`
- Additional directories (if mounted) are available at `/additional/dirname`
- Can create/modify files as needed
- Must follow project conventions
- Should coordinate to avoid conflicts

### Directory Structure
- **Main workspace**: `/workspace` - Your primary project directory
- **Additional directories**: `/additional/` - Extra mounted directories for reference
  - Example: If dhan-api is mounted, access it at `/additional/dhan-api`

### Success Metrics
- Task completed as specified
- All tests passing
- Code quality standards met
- Comprehensive documentation created
- GitHub repository updated with all changes

## 📝 DELIVERABLES

### Required Outputs:
1. **Working Implementation** - Fully functional code
2. **Test Suite** - Comprehensive tests via unit-tester
3. **Documentation** - Complete docs via documentation-agent
4. **GitHub Repository** - All changes committed
5. **Summary Report** - Final status and instructions

### Quality Standards:
- Clean, consistent code across all files
- Proper error handling and validation
- Performance optimization where needed
- Security best practices followed
- Comprehensive inline documentation

## 🚀 EXECUTION COMMAND

The task supervisor should now:
1. Analyze this task comprehensively
2. Identify all required agents
3. Create execution plan
4. Deploy agents in coordinated fashion
5. Monitor and adjust as needed
6. Ensure quality throughout
7. Deliver complete solution

**Focus**: Quality over everything. Use all resources needed.
**Priority**: Project-level agents > Personal agents > Global agents
**Documentation**: Every decision and action must be documented
**Testing**: Every feature must be tested
**Backup**: Every milestone must be saved

Begin execution with task-supervisor-autonomous analyzing the complete requirements.