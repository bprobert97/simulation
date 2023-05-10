# Contact Graph Scheduling
Contact Graph Scheduling (CGS) is a method of assigning pick-up and delivery schedules in delay- and disruption tolerant networks. The principle application is that of assigning image capture and delivery in intermittently connected satellite networks, in which both tasks and image data are routed through the network via ground-space, space-space and space-ground contact opportunities.

![image](https://user-images.githubusercontent.com/70593134/237059116-ff0e9cca-946b-4afe-9c63-03810ceb3292.png)


## Codebase
This codebase includes a Discrete Event Simulation (DES) implementation of a Space Network, in which requests for location-specific images are submitted to a central scheduler. This scheduler implements CGS to define tasks that are issue throughout the constellation, such that task assignees receive the request in good time to execute the image acquisition, and use [Contact Graph Routing](https://www.sciencedirect.com/science/article/pii/S1084804520303489) to deliver data to the ground in the minimum time possible.

## Discrete Event Simulation
The DES is established using the [simpy](https://simpy.readthedocs.io/en/latest/) library, whereby image requests are submitted based on the invocation of a generator function that yields Request objects according to some probability distribution. The submission of Request objects triggers the CGS procedure on the Scheduler node, which, if successful, adds a Task object to the task table. This task table is then distributed throughout the network.

A simplfied representation of the Contact Controller function, which lives on board each node in the network, is presented below. The Contact Controller handles the Contact Procedure when two nodes are communicating, and waits when not. Multiple instances of the Contact Controller may be running at any one time, if deemed feasible on the node, for example if both an inter-satellite and space-ground contact are happening simultaneously. Within the Contact Procedure, the Send Bundle event is triggered if the necessary conditions are met, else a wait time is imposed before again checking the data buffer, or the process is exited.

![image](https://user-images.githubusercontent.com/70593134/237060270-48b4a3af-2329-499c-bd8b-1db4505c99ea.png)

# Executing the model
There are two options for executing the CGS simulation
 1. Single-scenario execution
 2. Multiple-scenario execution

## Single-scenario
To run a single scenarion, do the following:
 1. At the bottom of the `main.py` file, set the `filename` variable to be the path location of the JSON file containing the mission definition to be evaluated.
 2. Run the `main.py` file

Some summary analysis of the simulation will be displayed in the console, and a pickled version of the Analytics object saved in the directory `/results/single/{filename}`

## Multi-scenario
Multiple scenarios can be run from a single execution, to provide additional convenience when performing trade-off analyses. The multiple scenarios must be based off of a single input file (JSON), with specific attributes that contribute to the problem space, being defined manually. 

### Execute the simulations
To run:
 1. Open the `mainMulty.py` file
 2. Comment/Uncomment (as appropriate) the values for **schemes**, **uncertainties** 
    and **congestions**. These lists define the possible design options and a 
    full-factorial analysis will be carried out (i.e. every possible combination of 
    entries)
 3. Define the base input file to be used for the simulation, in the `filename` variable
 4. Run the `mainMulti.py` file, which will iterate over each of the combinations and 
    save a Pickle file, per simulation, into the `/results/multi` directory.
 5. If required, manually move the new results files into their own directory

**Note** that the value for _traffic.congestion_ and _traffic.msr_ get updated 
automatically during the execution, as defined by the **congestions** and **schemes** 
item, respectively.

### To plot the results
It is recommended that the plotting is done either for a set of scheduling _schemes_ 
at a particular level of _uncertainty_, or a single _scheme_ at different 
_uncertainties_. To set up plotting, in the `plotResults.py` file:
 1. Ensure only one of either the `scheme` or `uncertainty` variable (list) has 
    multiple entries,  with the other just having a single entry.
 2. Update the `rsls` variable to match the `congestions` used in the inputs
 3. Update the `scheme` and `uncertainty` lists to match those defined in the 
    simulation inputs
 4. Modify the `filename` variable to match the path to the results files
 5. Run the `plotResults.py` file
