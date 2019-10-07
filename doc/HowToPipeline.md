# The ins and outs of making a PipelineTask
## Table of Contents
- [Getting started](#getting-started)
- [The ins and outs of making a PipelineTask](#the-ins-and-outs-of-making-a-pipelinetask)
- [Arguments and returns for \_\_init\_\_](#arguments-and-returns-for-\_\_init\_\_)
- [Optional Data-sets](#optional-data-sets)
- [Data-set name configuration and templates](#data-set-name-configuration-and-templates)
- [Prerequisite inputs](#prerequisite-inputs)
- [Processing multiple data-sets of the same data-set type](#processing-multiple-data-sets-of-the-same-data-set-type)
- [Deferred Loading](#deferred-loading)
- [Altering what is processed prior to processing](#altering-what-is-processed-prior-to-processing)- [Overriding Task execution](#overriding-task-execution)
- [Glossary](#glossary)
  - [Activator](#activator)
  - [Registry](#registry)
  - [Dimension](#dimension)
  - [Quantum](#quantum)
  - [Storage Classes](#storage-classes)

## Getting started
So you want to build a PipelineTask, where should you begin? In this guide of course, that's why you are here
reading it isn't it? This guide will create a new PipelineTask to measure aperture photometry, using
progressively more features of Gen3 middleware. So let's begin.

A PipelineTask is at its heart just a Task, so the best place to start in creating a task is to create a
Config class.

```python
import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.afw.table as afwTable
import lsst.afw.image as afwImage

class ApertureTaskConfig(pipeBase.Config):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4) 
```

So now you have a Config, let's make a Task to actually do the measurements

```python
class ApertureTask(pipeBase.Task):

    ConfigClass = ApertureTaskConfig
    
    def __init__(self, config: pipeBase.Config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.apRad = self.config.apRad
    
        self.outputSchema = afwTable.SourceTable.makeMinimalSchema()
        self.apKey = self.outputSchema.addField("apFlux", type=np.float64, doc="Ap flux measured")
    
        self.outputCatalog = afwTable.SourceCatalog(self.outputSchema)

    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog) -> pipeBase.Struct:
        # Get the dimensions of the exposure
        dimensions = exposure.getDimensions()

        # Get indexes for each pixel
        indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

        # Loop over each record in the catalog
        for source in inputCatalog:
            # Create an aperture and measure the flux
            center = source.getCentroid()
            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Add a record to the output catalog
            tmpRecord = self.outputCatalog.addNew()
            tmpRecord.set(self.apKey, flux)
        
        return self.outputCatalog
```

So now you have a task that takes an exposure, and inputCatalog in it's run method and returns a new output
catalog with apertures measured. As with all Tasks, this will work well for in memory data products, but you
want to write a PipelineTask, so you want to do some IO. To do this we need to have our task inherit from
PipelineTask primitives instead of base objects. Let's start by converting our Config class.

```python
class ApertureTaskConfig(pipeBase.PipelineTaskConfig):
    ...
```

Now however when we try and import the module containing this config, an exception is thrown complaining about
a missing PipelineTaskConnections class. What is a PipelineTaskConnection class anyway? In generation 3
middleware Tasks that do IO need to declare what kind of data-products they need, what kind they will
produce, and what identifiers are used to fetch that data. Additionally PipelineTaskConnections defined a unit
of work over which the task will operate, such as measurements on coadds will work on an individual Tracts,
Patches, and Filter. By declaring this information, the generation 3 middleware is able to orchestrate the
loading, saving, and running of one or more tasks. With that in mind, let's create a connection class for our
new Task. We expect that this task is going to work on individual ccds, looking at processed calexps and
associated measurement catalogs (that were produced by some previous task).

```python
class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
   exposure = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                             dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                             storageClass="ExposureF",
                                             name="calexp")
   inputCatalog = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                 dimensions=("visit", "detector", "abstract_filter",
                                                             "skymap"),
                                                 storageClass="SourceCatalog",
                                                 name="src") 
   outputCatalog = pipeBase.connectionTypes.Output(doc="Aperture measurements",
                                                   dimensions=("visit", "detector", "abstract_filter",
                                                               "skymap"),
                                                   storageClass="SourceCatalog",
                                                   name="customAperture")
```

So what is going on here? The first thing that happens is that ApertureTaskConnections inherits from
PipelineTaskConnections, that is fairly standard. What might be new syntax for you is the `dimensions=...`.
This is how we tell the connections class what unit of work a Task that uses this connection class will
operate on. The unit of work for a `PipelineTask` is known as a `quantum`. In the case of our Task, it will
work on a `visit` (a single image taken by the camera), `detector` (an individual ccd out of the cameras
mosaic), `abstract_filter` (an abstract notion of a filter, say r band, that is not tied to the exact band
passes of an individual telescope filter), and a `skymap` (a particular tesselation of the sky into regions)
at a time. Next we look at the fields defined on our new connection class. These are defined in a similar
way as defining a configuration class, but instead of using `Field`s from `pex_config`, we make use of
connection types defined in `pipe_base`. These connections define the inputs and outputs that a `PipelineTask`
will expect to make use of. Each of these connections documents what the connection is, what dimensions
represent this data product (in this case they are the same as the task itself, in the [Processing multiple
data-sets of the same data-set type](#processing-multiple-data-sets-of-the-same-data-set-type) section we
will cover when they are different), what kind of storage class represents this data type on disk (more on
storage classes in the glossary) and the name of the data type itself. In this connections class we have
defined two inputs and an output. The inputs the Task will make use of are `calexp`s and `src` catalogs,
both of which are produced by the `CalibratePipelineTask` during single frame processing. Our Task will
produce a new SourceCatalog of aperture measurements and save it out with a data-set type we have named
`customAperture`.

So now we have a ConnectionsClass, how we we use it? The good news is that we only need to make one small
change to our ApertureTaskConfig.

```python
class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                         pipelineConnections=ApertureTaskConnections):
    ...
```

That's it. All the rest of the Config class stays the same. Below we will give examples of what this does, and
how to use it, but first lets take a look at what changes when we turn a Task into a PipelineTask.

```python
class ApertureTask(pipeBase.PipelineTask):

    ...
    
    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog) -> pipeBase.Struct:
            ...
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

In a simple PipelineTask like this, these are all the changes that need be made. Firstly we changed the
base class to `PipelineTask`. This inheritance provides all the base machinery that the gen3 middleware
will need to run this task. The second change we made was to the signature of the run method. A run method
in a PipelineTask must return a `pipe_base` `Struct` object whose field names correspond to the names of
the outputs defined in the connection class. In our connection class we defined the output collection with
the identifier `outputCatalog`, so in our returned `Struct` has a field with that name as well. Another
thing worth highlighting, though it was not a change that was made, is the names of the arguments to the
run method. These names also must (and do) correspond to the identifiers used for the input connections.
The names of the variables of the inputs and outputs are how the `PipelineTask` activator maps connections
into the in memory data-products that the algorithm requires.

Putting it all together:

```python
import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.afw.table as afwTable
import lsst.afw.image as afwImage


class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    exposure = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                              dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                              storageClass="ExposureF",
                                              name="calexp")
    inputCatalog = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                  dimensions=("visit", "detector", "abstract_filter",
                                                             "skymap"),
                                                  storageClass="SourceCatalog",
                                                  name="src") 
    outputCatalog = pipeBase.connectionTypes.Output(doc="Aperture measurements",
                                                    dimensions=("visit", "detector", "abstract_filter",
                                                                "skymap"),
                                                    storageClass="SourceCatalog",
                                                    name="customAperture")


class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                         pipelineConnections=ApertureTaskConnections):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4) 


class ApertureTask(pipeBase.PipelineTask):

    ConfigClass = ApertureTaskConfig
    
    def __init__(self, config: pipeBase.Config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.apRad = self.config.apRad
    
        self.outputSchema = afwTable.SourceTable.makeMinimalSchema()
        self.apKey = self.outputSchema.addField("apFlux", type=np.float64, doc="Ap flux measured")
    
        self.outputCatalog = afwTable.SourceCatalog(self.outputSchema)
    
    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog) -> pipeBase.Struct:
        # Get the dimensions of the exposure
        dimensions = exposure.getDimensions()

        # Get indexes for each pixel
        indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

        # Loop over each record in the catalog
        for source in inputCatalog:
            # Create an aperture and measure the flux
            center = source.getCentroid()
            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Add a record to the output catalog
            tmpRecord = self.outputCatalog.addNew()
            tmpRecord.set(self.apKey, flux)
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

## Arguments and returns for \_\_init\_\_
Let's make this class a little bit more advanced. Instead of creating a new catalog that only contains the
aperture flux measurements, lets create a catalog that contains all the input data, and adds an additional
column for our aperture measurements. To do this we need the schema for the input catalog in our init method
so that we can correctly construct our output catalog to contain all the appropriate fields. This is
accomplished by using the InitInput connection type in the connection class. Data products specified with this
connection type will be provided to the `__init__` method of a task in a dictionary named `initInputs`. Let's
see what this looks like.

```python
class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    inputSchema = pipeBase.connectionTypes.InitInput(doc="Schema associated with a src catalog",
                                                     storageClass="SourceCatalog",
                                                     name="src_schema")
    ...

class ApertureTask(pipeBase.PipelineTask):

    ...
    
    def __init__(self, config: pipeBase.Config, initInput, *args, **kwargs):
        ...
        inputSchema = initInput['inputSchema'].schema
    
        # Create a camera mapper to create a copy of the input schema
        self.mapper = afwTable.SchemaMapper(inputSchema)
        self.mapper.addMinimalSchema(inputSchema, True)

        # Add our new field
        self.apKey = self.mapper.editOutputSchema().addField("apFlux", type=np.float64,
                                                             doc="Ap flux measured")
        
        # Get the output schema
        self.outputSchema = self.mapper.getOutputSchema()
    
        # create the catalog in which new measurements will be stored
        self.outputCatalog = afwTable.SourceCatalog(self.outputSchema)

    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog) -> pipeBase.Struct:
        # Add in all the records from the input catalog into what will be the output catalog
        self.outputCatalog.extend(inputCatalog, mapper=self.mapper)

        # Get the dimensions of the exposure
        dimensions = exposure.getDimensions()

        # Get indexes for each pixel
        indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

        # Loop over each record in the catalog
        for source in outputCatalog:
            # Create an aperture and measure the flux
            center = source.getCentroid()
            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Set the flux field of this source
            source.set(self.apKey, flux)
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

Our changes allow us to load in and use schemas to initialize our task before any actual data is loaded to
be passed to the algorithm code located in the `run` method. Inside the `run` method, the output catalog
copies all the records from the input into itself. The loop can the go over the output catalog, and insert
the new measurements right into the output catalog.

One thing to note about `InitInput` connections is that they do not take any dimensions. This is because
the sort of data loaded will correspond to a given data-set type produced by a task, and not by (possibly
multiple) executions of a run method over data-sets that have dimensions, that is to say these data-sets
are unique to the task itself and not tied to the unit of work that the task operates on.

In the same way we added in the schema from some previous stage of processing, we should write out our output
schema, so some other `PipelineTask` can make use of it. To do this we add an `InitOutput` connection to our
connection class.

```python
class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    ...
    outputSchema = pipeBase.connectionTypes.InitOutput(doc="Schema generated in Aperture PipelineTask",
                                                       storageClass="SourceCatalog",
                                                       name="customAperture_schema")
                                                       
    ...


class ApertureTask(pipeBase.PipelineTask):

    ...
    
    def __init__(self, config: pipeBase.Config, initInput, *args, **kwargs):
        ...
        # Get the output schema
        self.schema = mapper.getOutputSchema()
    
        # Create the output catalog
        self.outputCatalog = afwTable.SourceCatalog(self.schema)

        # Put the outputSchema into a SourceCatalog container. This var name matches an initOut
        # so will be persisted
        self.outputSchema = afwTable.SourceCatalog(self.schema)
```

In the init method we associate the variable we would like to output with a name that matches the variable
name used in the connection class. The activator uses this shared name to know which variable should be
persisted.

Bringing it all together we get the following,

```python
import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.afw.table as afwTable
import lsst.afw.image as afwImage


class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    inputSchema = pipeBase.connectionTypes.InitInput(doc="Schema associated with a src catalog",
                                                     storageClass="SourceCatalog",
                                                     name="src_schema")
    outputSchema = pipeBase.connectionTypes.InitOutput(doc="Schema generated in Aperture PipelineTask",
                                                       storageClass="SourceCatalog",
                                                       name="customAperture_schema")
    exposure = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                              dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                              storageClass="ExposureF",
                                              name="calexp")
    inputCatalog = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                  dimensions=("visit", "detector", "abstract_filter",
                                                              "skymap"),
                                                  storageClass="SourceCatalog",
                                                  name="src") 
    outputCatalog = pipeBase.connectionTypes.Output(doc="Aperture measurements",
                                                    dimensions=("visit", "detector", "abstract_filter",
                                                                "skymap"),
                                                    storageClass="SourceCatalog",
                                                    name="customAperture")


class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                         pipelineConnections=ApertureTaskConnections):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4) 


class ApertureTask(pipeBase.PipelineTask):

    ConfigClass = ApertureTaskConfig
    
    def __init__(self, config: pipeBase.Config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.apRad = self.config.apRad

        inputSchema = initInput['inputSchema'].schema
    
        # Create a camera mapper to create a copy of the input schema
        self.mapper = afwTable.SchemaMapper(inputSchema)
        self.mapper.addMinimalSchema(inputSchema, True)

        # Add our new field
        self.apKey = self.mapper.editOutputSchema().addField("apFlux", type=np.float64,
                                                             doc="Ap flux measured")

        # Get the output schema
        self.schema = mapper.getOutputSchema()
    
        # Create the output catalog
        self.outputCatalog = afwTable.SourceCatalog(self.schema)

        # Put the outputSchema into a SourceCatalog container. This var name matches an initOut
        # so will be persisted
        self.outputSchema = afwTable.SourceCatalog(self.schema)

    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog) -> pipeBase.Struct:
        # Add in all the records from the input catalog into what will be the output catalog
        self.outputCatalog.extend(inputCatalog, mapper=self.mapper)

        # Get the dimensions of the exposure
        dimensions = exposure.getDimensions()

        # Get indexes for each pixel
        indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

        # Loop over each record in the catalog
        for source in outputCatalog:
            # Create an aperture and measure the flux
            center = source.getCentroid()
            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Set the flux field of this source
            source.set(self.apKey, flux)
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

## Optional Data-sets
Sometimes it is useful to have a task that optionally uses a data-set. In the case of our example task this
might be a background model that was previously removed. We may want our task to add back in the background
so that we can do a new local background estimate. To start let's add the background data-set to our
connection class like we did our other data-sets. 

```python
class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    ...
    background = pipeBase.connectionTypes.Input(doc="Background model for the exposure",
                                                storageClass="Background",
                                                name="calexpBackground",
                                                dimensions=("visit", "detector", "abstract_filter", "skymap"))
    ...
```

Now our `PipelineTask` will load the background each time the task is run. How do we make this optional?
First we will add a configuration field in our config class to allow the user to specify if it is to be
loaded like thus

```python
class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                         pipelineConnections=ApertureTaskConnections):
    ...
    doLocalBackground = pexConfig.Field(doc="Should the background be added before doing photometry",
                                        dtype=bool, default=False)
```

The `\_\_init\_\_` method of the connection class is given an instance of the tasks config class after all
overrides have been applied. This provides us an opportunity change the behavior of the connection class
according to various config options.

```python
class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    ...
    background = pipeBase.connectionTypes.Input(doc="Background model for the exposure",
                                                storageClass="Background",
                                                name="calexpBackground",
                                                dimensions=("visit", "detector", "abstract_filter", "skymap"))
    ...

    def __init__(self, *, config=None):
        super().__init__(config=config)
        
        if config.doLocalBackground is False:
            self.inputs.remove("background")
```

Our connection class now looks at the value of `doLocalBackground` on the config object and if it is false,
removes it from the connection instances list of input connections. Connection classes keep track of what
connections are defined in sets. Each set contains the variable names of a connection, and the sets
themselves are identified by the type of connection they contain. In our example we are modifying the set
of input connections. The names for each of the sets are as follows:

* initInputs
* initOutputs
* inputs
* prerequisiteInputs
* outputs 

The last step in modifying our task will be to update the run method to take into account that a background
may or may not be supplied.

```python
...
import typing

import lsst.afw.math as afwMath

...

class ApertureTask(pipeBase.PipelineTask):
    
    ...

    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog,
            background: typing.Union[None, afwMath.BackgroundList] = None) -> pipeBase.Struct:
        # If a background is supplied, add it back to the image so local background subtraction
        # can be done.
        if background is not none:
            exposure.image.array += background.getImage()

        ...

        # Loop over each record in the catalog
        for source in outputCatalog:

            ...

            # Do local background subtraction
            if background is not None:
                outerAn = distance < 2.5*self.apRad
                innerAn = distance < 1.5*self.apRad
                annulus = outerAn - innerAn
                localBackground = np.mean(exposure.image.array*annulus)
                flux -= np.sum(mask)*localBackground

            ...
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

The run method now takes an argument named background, which defaults to a value of `None`. If the
connection is removed from the connection class, there will be no argument passed to run with that name.
Conversely, when the connection is present the background is un-persisted by the butler, and is passed on
to the run method. The body of the run method checks if the background has been passed, and if so adds it
back in and does a local background subtraction.

Bringing it all together,

```python
import typing

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.afw.table as afwTable
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath


class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    inputSchema = pipeBase.connectionTypes.InitInput(doc="Schema associated with a src catalog",
                                                     storageClass="SourceCatalog",
                                                     name="src_schema")
    outputSchema = pipeBase.connectionTypes.InitOutput(doc="Schema generated in Aperture PipelineTask",
                                                       storageClass="SourceCatalog",
                                                       name="customAperture_schema")
    exposure = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                              dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                              storageClass="ExposureF",
                                              name="calexp")
    inputCatalog = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                  dimensions=("visit", "detector", "abstract_filter",
                                                              "skymap"),
                                                  storageClass="SourceCatalog",
                                                  name="src") 
    background = pipeBase.connectionTypes.Input(doc="Background model for the exposure",
                                                storageClass="Background",
                                                name="calexpBackground",
                                                dimensions=("visit", "detector", "abstract_filter", "skymap"))
    outputCatalog = pipeBase.connectionTypes.Output(doc="Aperture measurements",
                                                    dimensions=("visit", "detector", "abstract_filter",
                                                                "skymap"),
                                                    storageClass="SourceCatalog",
                                                    name="customAperture")

    def __init__(self, *, config=None):
        super().__init__(config=config)
        
        if config.doLocalBackground is False:
            self.inputs.remove("background")


class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                         pipelineConnections=ApertureTaskConnections):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4) 
    doLocalBackground = pexConfig.Field(doc="Should the background be added before doing photometry",
                                        dtype=bool, default=False)


class ApertureTask(pipeBase.PipelineTask):

    ConfigClass = ApertureTaskConfig
    
    def __init__(self, config: pipeBase.Config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.apRad = self.config.apRad

        inputSchema = initInput['inputSchema'].schema
    
        # Create a camera mapper to create a copy of the input schema
        self.mapper = afwTable.SchemaMapper(inputSchema)
        self.mapper.addMinimalSchema(inputSchema, True)

        # Add our new field
        self.apKey = self.mapper.editOutputSchema().addField("apFlux", type=np.float64,
                                                             doc="Ap flux measured")

        # Get the output schema
        self.schema = mapper.getOutputSchema()
    
        # Create the output catalog
        self.outputCatalog = afwTable.SourceCatalog(self.schema)

        # Put the outputSchema into a SourceCatalog container. This var name matches an initOut
        # so will be persisted
        self.outputSchema = afwTable.SourceCatalog(self.schema)

    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog,
            background: typing.Union[None, afwMath.BackgroundList] = None) -> pipeBase.Struct:
        # If a background is supplied, add it back to the image so local background subtraction
        # can be done.
        if background is not none:
            exposure.image.array += background.getImage()

        # Add in all the records from the input catalog into what will be the output catalog
        self.outputCatalog.extend(inputCatalog, mapper=self.mapper)

        # Get the dimensions of the exposure
        dimensions = exposure.getDimensions()

        # Get indexes for each pixel
        indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

        # Loop over each record in the catalog
        for source in outputCatalog:
            # Create an aperture and measure the flux
            center = source.getCentroid()
            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Do local background subtraction
            if background is not None:
                outerAn = distance < 2.5*self.apRad
                innerAn = distance < 1.5*self.apRad
                annulus = outerAn - innerAn
                localBackground = np.mean(exposure.image.array*annulus)
                flux -= np.sum(mask)*localBackground

            # Set the flux field of this source
            source.set(self.apKey, flux)
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

## Data-set name configuration and templates 
Now that we have the option to control results of processing with a configuration option (turning on and
off local background subtraction) it may be useful for a user who turns on local background subtraction to
change the name of the data-set produced so as to tell what the configuration was without looking at the
persisted configs. The user may make a config override file that looks something like the following.

```python
config.doLocalBackground = True
config.connections.outputSchema = "customAperture_localBg_schema"
config.connections.outputCatalog = "customAperture_localBg"
```

This config file introduces the special attribute connections that exists on every `PipelineTaskConfig`
class. This attribute is dynamically built from the linked `PipelineTaskConnections` class. The
`connections` attribute is a sub-config that has fields corresponding to the variable names of the
connections, with values of those fields corresponding to the name of a connection. So by default
`config.connections.outputCatalog` would be `customAperture` and `config.connections.exposure` would be
`calexp` etc. Assigning to these config fields has the effect of changing the name of the data-set type
defined in the connection.

In this config file we are changing the name of the data-set type that will be persisted to include the
information that local background subtraction was done. It is interesting to note that there are no hard
coded data-set type names that must be adhered to, the user is free to pick any name. The only consequence
of changing a data-set type name, is that any down stream code that is to use the output data-set must have
its default name changed to match. As an aside, the current activator requires that the first time a
data-set name is used the activator command is run with the `--register-dataset-types` switch. This is to
prevent accidental typos becoming new data-set types.

Looking at the config file, there are two different fields that we are setting to the same value. In the
case of other tasks this number may even be higher. This leads not only to the issue of config overrides
needing to potentially be lengthy, but that there may be a typo, and the fields will be set inconsistently.
To address this, `PipelineTasks` have a way to template data-set type names. Let's see what this looks
like.

```python
class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              defaultTemplates={"outputName":"customAperture"},
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    ...

    outputSchema = pipeBase.connectionTypes.InitOutput(doc="Schema generated in Aperture PipelineTask",
                                                       storageClass="SourceCatalog",
                                                       name="{outputName}_schema")
    
    ...

    outputCatalog = pipeBase.connectionTypes.Output(doc="Aperture measurements",
                                                    dimensions=("visit", "detector", "abstract_filter",
                                                                "skymap"),
                                                    storageClass="SourceCatalog",
                                                    name="{outputName}")
```

In the modified connection class, the `outputSchema` and `outputCatalog` connections now have python format
strings, which are referred to with the template name `outputName`. This template will formatted a
specified string to become a data-set type name prior to any data being read and supplied to the task. The
class declaration also has a new argument along side the `dimensions` argument, `defaultTemplates`. This
template string is what will be used to format the name string if a user does not provide any overrides.
The defaults are supplied as a python dictionary of template identifiers as keys, and default strings as
values. If there are any templates used in a connection class, a default template must be supplied for each
template identifier. A `TypeError` will be thrown if you attempt to import a module containing a
`PipelineTaskConnections` class that does not have defaults for all the defined templates.

With these changes, let's take a look at how our config override file changes.

```python
config.doLocalBackground = True
config.connections.outputName = "customAperture_localBg"
```

The `connections` sub-config now contains a field called `outputName`, the same as our template identifier.
Each template identifier will have a corresponding field on the `connections` sub-config. Setting the value
on these configs has the effect of setting the templates where ever they are used.

Setting a template config field does not preclude also setting the name of a data-set type directly, which
may be useful in cases with templates used in lots of places. Though not needed in this example, such a
config would look something like the following.

```python
config.doLocalBackground = True
config.connections.outputName = "customAperture_localBg"
config.connections.outputSchema = "different_name_schema"
```

The complete picture of the task is now this:

```python
import typing

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.afw.table as afwTable
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath


class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              defaultTemplates={"outputName":"customAperture"},
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    inputSchema = pipeBase.connectionTypes.InitInput(doc="Schema associated with a src catalog",
                                                     storageClass="SourceCatalog",
                                                     name="src_schema")
    outputSchema = pipeBase.connectionTypes.InitOutput(doc="Schema generated in Aperture PipelineTask",
                                                       storageClass="SourceCatalog",
                                                       name="{outputName}_schema")
    exposure = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                              dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                              storageClass="ExposureF",
                                              name="calexp")
    inputCatalog = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                  dimensions=("visit", "detector", "abstract_filter",
                                                              "skymap"),
                                                  storageClass="SourceCatalog",
                                                  name="src") 
    background = pipeBase.connectionTypes.Input(doc="Background model for the exposure",
                                                storageClass="Background",
                                                name="calexpBackground",
                                                dimensions=("visit", "detector", "abstract_filter",
                                                            "skymap"))
    outputCatalog = pipeBase.connectionTypes.Output(doc="Aperture measurements",
                                                    dimensions=("visit", "detector", "abstract_filter",
                                                               "skymap"),
                                                    storageClass="SourceCatalog",
                                                    name="{outputName}")

    def __init__(self, *, config=None):
        super().__init__(config=config)
        
        if config.doLocalBackground is False:
            self.inputs.remove("background")


class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                         pipelineConnections=ApertureTaskConnections):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4) 
    doLocalBackground = pexConfig.Field(doc="Should the background be added before doing photometry",
                                        dtype=bool, default=False)


class ApertureTask(pipeBase.PipelineTask):

    ConfigClass = ApertureTaskConfig
    
    def __init__(self, config: pipeBase.Config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.apRad = self.config.apRad

        inputSchema = initInput['inputSchema'].schema
    
        # Create a camera mapper to create a copy of the input schema
        self.mapper = afwTable.SchemaMapper(inputSchema)
        self.mapper.addMinimalSchema(inputSchema, True)

        # Add our new field
        self.apKey = self.mapper.editOutputSchema().addField("apFlux", type=np.float64,
                                                             doc="Ap flux measured")

        # Get the output schema
        self.schema = mapper.getOutputSchema()
    
        # Create the output catalog
        self.outputCatalog = afwTable.SourceCatalog(self.schema)

        # Put the outputSchema into a SourceCatalog container. This var name matches an initOut
        # so will be persisted
        self.outputSchema = afwTable.SourceCatalog(self.schema)

    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog,
            background: typing.Union[None, afwMath.BackgroundList] = None) -> pipeBase.Struct:
        # If a background is supplied, add it back to the image so local background subtraction
        # can be done.
        if background is not none:
            exposure.image.array += background.getImage()

        # Add in all the records from the input catalog into what will be the output catalog
        self.outputCatalog.extend(inputCatalog, mapper=self.mapper)

        # Get the dimensions of the exposure
        dimensions = exposure.getDimensions()

        # Get indexes for each pixel
        indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

        # Loop over each record in the catalog
        for source in outputCatalog:
            # Create an aperture and measure the flux
            center = source.getCentroid()
            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Do local background subtraction
            if background is not None:
                outerAn = distance < 2.5*self.apRad
                innerAn = distance < 1.5*self.apRad
                annulus = outerAn - innerAn
                localBackground = np.mean(exposure.image.array*annulus)
                flux -= np.sum(mask)*localBackground

            # Set the flux field of this source
            source.set(self.apKey, flux)
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

## Prerequisite inputs
Some tasks make use of data-sets that are created outside the processing environment of LSST data-sets.
These may be things like reference catalogs, bright star masks, calibrations, etc. To account for this
PipelineTasks have a special type of connection called, `PrerequisiteInput`. This type of input tells the
execution system, that this is a special type of data-set, and not to try and expect it to be produced
anywhere in a processing pipeline. If this data-set is not found, the system will raise a hard error and
tell you the data-set type is missing instead of inferring that there is some processing step that is
missing.

These connections are specified the same way as any input, but with a different connection type name. For
our example, we will add in a prerequisite type on a mask, that is assumed to be human created. If a source
center happens to fall in a masked area, no aperture photometry will be performed. Because this is almost
the same as an Input connection type, the complete updated example is shown below, in lue of building it up
a piece at a time.

```python
import math
import typing

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.afw.table as afwTable
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath


class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              defaultTemplates={"outputName":"customAperture"},
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    inputSchema = pipeBase.connectionTypes.InitInput(doc="Schema associated with a src catalog",
                                                     storageClass="SourceCatalog",
                                                     name="src_schema")
    outputSchema = pipeBase.connectionTypes.InitOutput(doc="Schema generated in Aperture PipelineTask",
                                                       storageClass="SourceCatalog",
                                                       name="{outputName}_schema")
    areaMask = pipeBase.connectionTypes.PrerequisiteInput(doc="A mask of areas to be ignored",
                                                          storageClass="Mask",
                                                          dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                                          name="ApAreaMask")
    exposure = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                              dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                              storageClass="ExposureF",
                                              name="calexp")
    inputCatalog = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                  dimensions=("visit", "detector", "abstract_filter",
                                                              "skymap"),
                                                  storageClass="SourceCatalog",
                                                  name="src") 
    background = pipeBase.connectionTypes.Input(doc="Background model for the exposure",
                                                storageClass="Background",
                                                name="calexpBackground",
                                                dimensions=("visit", "detector", "abstract_filter", "skymap"))
    outputCatalog = pipeBase.connectionTypes.Output(doc="Aperture measurements",
                                                    dimensions=("visit", "detector", "abstract_filter",
                                                                "skymap"),
                                                    storageClass="SourceCatalog",
                                                    name="{outputName}")

    def __init__(self, *, config=None):
        super().__init__(config=config)
        
        if config.doLocalBackground is False:
            self.inputs.remove("background")


class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                         pipelineConnections=ApertureTaskConnections):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4) 
    doLocalBackground = pexConfig.Field(doc="Should the background be added before doing photometry",
                                        dtype=bool, default=False)


class ApertureTask(pipeBase.PipelineTask):

    ConfigClass = ApertureTaskConfig
    
    def __init__(self, config: pipeBase.Config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.apRad = self.config.apRad

        inputSchema = initInput['inputSchema'].schema
    
        # Create a camera mapper to create a copy of the input schema
        self.mapper = afwTable.SchemaMapper(inputSchema)
        self.mapper.addMinimalSchema(inputSchema, True)

        # Add our new field
        self.apKey = self.mapper.editOutputSchema().addField("apFlux", type=np.float64,
                                                             doc="Ap flux measured")

        # Get the output schema
        self.schema = mapper.getOutputSchema()
    
        # Create the output catalog
        self.outputCatalog = afwTable.SourceCatalog(self.schema)

        # Put the outputSchema into a SourceCatalog container. This var name matches an initOut
        # so will be persisted
        self.outputSchema = afwTable.SourceCatalog(self.schema)

    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog,
            areaMask: afwImage.Mask,
            background: typing.Union[None, afwMath.BackgroundList] = None,) -> pipeBase.Struct:
        # If a background is supplied, add it back to the image so local background subtraction
        # can be done.
        if background is not none:
            exposure.image.array += background.getImage()

        # Add in all the records from the input catalog into what will be the output catalog
        self.outputCatalog.extend(inputCatalog, mapper=self.mapper)

        # Get the dimensions of the exposure
        dimensions = exposure.getDimensions()

        # Get indexes for each pixel
        indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

        # Loop over each record in the catalog
        for source in outputCatalog:
            # Create an aperture and measure the flux
            center = source.getCentroid()

            # Skip measuring flux if the center of a source is in a masked pixel
            if areaMask.array[math.floor(center.getY()), math.floor(center.getX())] != 0:
                source.set(self.apKey, math.nan)
                continue

            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Do local background subtraction
            if background is not None:
                outerAn = distance < 2.5*self.apRad
                innerAn = distance < 1.5*self.apRad
                annulus = outerAn - innerAn
                localBackground = np.mean(exposure.image.array*annulus)
                flux -= np.sum(mask)*localBackground

            # Set the flux field of this source
            source.set(self.apKey, flux)
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

## Processing multiple data-sets of the same data-set type
The dimensions we have used in our task up to this point have specified that the unit of processing is to
be done on individual detectors on a pre visit basis. This makes sense, as this is a natural
parallelization vector. However, sometimes it is useful to consider an entire focal plane at a time, or some
other larger scale concept, like a tract. By changing the dimensions of the unit of processing for our
task, we by necessity change what sort of inputs our task will expect. Let's make that change now, and see
what the consequences will be to the connection class.

```python
class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              defaultTemplates={"outputName":"customAperture"},
                              dimensions=("visit", "abstract_filter", "skymap")):
    ...

    areaMasks = pipeBase.connectionTypes.PrerequisiteInput(doc="A mask of areas to be ignored",
                                                           storageClass="Mask",
                                                           dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                                           name="ApAreaMask",
                                                           multiple=True)
    exposures = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                               dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                               storageClass="ExposureF",
                                               name="calexp",
                                               multiple=True)
    inputCatalogs = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                   dimensions=("visit", "detector", "abstract_filter",
                                                               "skymap"),
                                                   storageClass="SourceCatalog",
                                                   name="src",
                                                   multiple=True) 
    backgrounds = pipeBase.connectionTypes.Input(doc="Background model for the exposure",
                                                 storageClass="Background",
                                                 dimensions=("visit", "detector", "abstract_filter",
                                                             "skymap"),
                                                 name="calexpBackground",
                                                 multiple=True)
    ...
```

The dimensions of our `ApertureTaskConnections` class are now `visit`, `abstract_filter`, and `skymap`.
However all of our input data-sets are themselves still defined over each of these dimensions, and also
`detector`. That is to say you get one `calexp` for ever unique combination of `exposure`'s dimensions.
Because the tasks's dimensions are a more inclusive set of dimensions (less specified) we expect that for a
given unit of processing, there will be multiple values for each of the input data-set types along the
`detector` dimension. For example, in LSST there will be 189 detectors in each visit. We indicate to the
activator framework that we expect there to be a list of data-sets for each input (and in this case
prerequisite input) by annotating the connection with the argument `multi`. This ensures the values passed
will be inside of a list container.

As a caveat, depending on the exact data that has been ingested/processed, there may only be one data-set
that matches this combination of dimensions (i.e. only one raw was ingested for a visit) but the `multi`
flag will still ensure that the system passes this one data-set along inside contained inside a list. This
ensures a uniform api to program against. Make note that we change the names of the connections marked with `multi` to reflect that they will potentially contain multiple values.

With this in mind, let's make the changes needed accommodate multiple data-sets in each inputs to the run
method. Because this task is inherently parallel over detectors, these modifications are not the most
natural way to code this behavior, but are done to demonstrate how to make use of the `multi` flag for
situations that are not so trivial.

```python
class ApertureTask(pipeBase.PipelineTask):

    ...

    def run(self, exposures: typing.List[afwImage.Exposure],
            inputCatalogs: typing.List[afwTable.SourceCatalog],
            areaMasks: typing.List[afwImage.Mask],
            backgrounds: typing.Union[None, typing.List[afwMath.BackgroundList]] = None,) -> pipeBase.Struct:
        # Add in all the input catalogs into the output catalog
        for inCat in inputCatalogs:
            self.outputCatalog.extend(inCat, mapper=self.mapper)

        # Track which input catalog a given source is coming from
        inputCatCount = 0
        cumLength = 0

        # Create some empty variables
        exposure, areaMask, dimensions, indy, indx = [None]*5

        # This function is an inner function (closure) and has access to variables defined outside
        # its scope. Based on what input catalog is being processed, it updates each of these
        # non local variables.
        def updateVars():
            nonlocal exposure, areaMask, dimensions, indy, indx, cumLength, inputCatCount
            exposure = exposure[inputCatCount]
            areaMask = areaMasks[inputCatCount]

            # If a background is supplied, add it back to the image so local background subtraction
            # can be done.
            if backgrounds is not none:
                exposure.image.array += backgrounds[inputCatCount].getImage()


            # Get the dimensions of the exposure
            dimensions = exposure.getDimensions()

            # Get indexes for each pixel
            indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

            cumLength += len(inputCatalogs[inputCatCount])
            inputCatCount += 1
        
        updateVars()

        # Loop over each record in the catalog
        for i, source in enumerate(outputCatalog):
            # If the source from this catalog comes from the next input catalog in the list,
            # update all the inputs
            if i > cumLength:
                updateVars() 

            # Create an aperture and measure the flux
            center = source.getCentroid()

            # Skip measuring flux if the center of a source is in a masked pixel
            if areaMask.array[math.floor(center.getY()), math.floor(center.getX())] != 0:
                source.set(self.apKey, math.nan)
                continue

            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Do local background subtraction
            if background is not None:
                outerAn = distance < 2.5*self.apRad
                innerAn = distance < 1.5*self.apRad
                annulus = outerAn - innerAn
                localBackground = np.mean(exposure.image.array*annulus)
                flux -= np.sum(mask)*localBackground

            # Set the flux field of this source
            source.set(self.apKey, flux)
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

The first change is to the signature of the run method, to reflect the change in the names of the
connections. Next, the output catalog must be extended to include all the supplied input catalogs. In order
to change the least amount of code necessary for this demo, the main loop is kept almost the same. To
account for the loop wanting a single value for things like the exposure or background, a function inside
of run is introduced to set these variables. This function is a closure around the variables to be updated,
and sets each of their values to correspond to the matching inputCatalog. When the loop proceeds through
the list of output sources, it checks if that source as come from the next inputCatalog in the list. If it
has then all of the other variables are updated to match. The specifics of this code are not as important
as the way in which they use the variables supplied to run, namely that the arguments are now list of
variables that each need handled.

### Deferred Loading

An astute eye will notice that this code will potentially consume a large amount of memory. Loading in
every catalog, image, background, and mask for an entire LSST focal plain will put a lot of pressure on the
memory of the computer running this code. Fortunately the LSST middleware gives us a way to lighten this
load. We can add an argument to a connection in our connection class that informs the activators to not
load the data at the time of running, but supply a variable that allows a task to load the data when the
task needs it. This argument is called `deferLoad`, and takes a boolean value which is by default false.
Let's take a look at our connection class with this in place.


```python
class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              defaultTemplates={"outputName":"customAperture"},
                              dimensions=("visit", "abstract_filter", "skymap")):
    ...

    areaMasks = pipeBase.connectionTypes.PrerequisiteInput(doc="A mask of areas to be ignored",
                                                           storageClass="Mask",
                                                           dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                                           name="ApAreaMask",
                                                           multiple=True,
                                                           deferLoad=True)
    exposures = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                               dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                               storageClass="ExposureF",
                                               name="calexp",
                                               multiple=True,
                                               deferLoad=True)
    inputCatalogs = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                   dimensions=("visit", "detector", "abstract_filter",
                                                              "skymap"),
                                                   storageClass="SourceCatalog",
                                                   name="src",
                                                   multiple=True,
                                                   deferLoad=True) 
    backgrounds = pipeBase.connectionTypes.Input(doc="Background model for the exposure",
                                                 storageClass="Background",
                                                 dimensions=("visit", "detector", "abstract_filter",
                                                             "skymap"),
                                                 name="calexpBackground",
                                                 multiple=True,
                                                 deferLoad=True)
    ...
```

Now Let's see how the `run` method changes to make use of this.


```python
class ApertureTask(pipeBase.PipelineTask):

    ...

    def run(self, exposures: typing.List[afwImage.Exposure],
            inputCatalogs: typing.List[afwTable.SourceCatalog],
            areaMasks: typing.List[afwImage.Mask],
            backgrounds: typing.Union[None, typing.List[afwMath.BackgroundList]] = None,) -> pipeBase.Struct:
        catalogLengths = []
        # Add in all the input catalogs into the output catalog
        for inCatHandle in inputCatalogs:
            inCat = inCatHandle.get()
            catalogLengths.append(len(inCat))
            self.outputCatalog.extend(inCat, mapper=self.mapper)

        # Track which input catalog a given source is coming from
        inputCatCount = 0
        cumLength = 0

        # Create some empty variables
        exposure, areaMask, dimensions, indy, indx = [None]*5

        # This function is an inner function (closure) and has access to variables defined outside
        # its scope. Based on what input catalog is being processed, it updates each of these
        # non local variables.
        def updateVars():
            nonlocal exposure, areaMask, dimensions, indy, indx, cumLength, inputCatCount
            exposure = exposure[inputCatCount].get()
            areaMask = areaMasks[inputCatCount].get()

            # If a background is supplied, add it back to the image so local background subtraction
            # can be done.
            if backgrounds is not none:
                exposure.image.array += backgrounds[inputCatCount].get().getImage()


            # Get the dimensions of the exposure
            dimensions = exposure.getDimensions()

            # Get indexes for each pixel
            indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

            cumLength += catalogLengths[inputCatCount]
            inputCatCount += 1
        
        updateVars()

        # Loop over each record in the catalog
        for i, source in enumerate(outputCatalog):
            # If the source from this catalog comes from the next input catalog in the list,
            # update all the inputs
            if i > cumLength:
                updateVars() 

            # Create an aperture and measure the flux
            center = source.getCentroid()

            # Skip measuring flux if the center of a source is in a masked pixel
            if areaMask.array[math.floor(center.getY()), math.floor(center.getX())] != 0:
                source.set(self.apKey, math.nan)
                continue

            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Do local background subtraction
            if background is not None:
                outerAn = distance < 2.5*self.apRad
                innerAn = distance < 1.5*self.apRad
                annulus = outerAn - innerAn
                localBackground = np.mean(exposure.image.array*annulus)
                flux -= np.sum(mask)*localBackground

            # Set the flux field of this source
            source.set(self.apKey, flux)
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

In this modified `run` method we need to track the lengths of each of our input catalogs, as they are not
in memory to refer to later, but the only other code addition is the use of the `get` method on input
arguments. When a connection is marked with `deferLoad`, the butler will supply an
`lsst.daf.butler.DeferredDatasetHandle`. This handle has a `get` method which loads and returns the object
specified by the handle. The `get` method also optionally supports a `parameters` argument that can be used
in the same manor as a normal `butler.get` call. This allows things like fetching only part of an image,
loading only the wcs from an exposure, etc. See butler documentation for more info on the parameters
argument.

Finally let's bring all this code together to see how our task now looks.


```python
import math
import typing

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.afw.table as afwTable
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath


class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              defaultTemplates={"outputName":"customAperture"},
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    inputSchema = pipeBase.connectionTypes.InitInput(doc="Schema associated with a src catalog",
                                                     storageClass="SourceCatalog",
                                                     name="src_schema")
    outputSchema = pipeBase.connectionTypes.InitOutput(doc="Schema generated in Aperture PipelineTask",
                                                       storageClass="SourceCatalog",
                                                       name="{outputName}_schema")
    areaMasks = pipeBase.connectionTypes.PrerequisiteInput(doc="A mask of areas to be ignored",
                                                           storageClass="Mask",
                                                           dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                                           name="ApAreaMask",
                                                           multiple=True,
                                                           deferLoad=True)
    exposures = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                               dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                               storageClass="ExposureF",
                                               name="calexp",
                                               multiple=True,
                                               deferLoad=True)
    inputCatalogs = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                   dimensions=("visit", "detector", "abstract_filter",
                                                               "skymap"),
                                                   storageClass="SourceCatalog",
                                                   name="src",
                                                   multiple=True,
                                                   deferLoad=True) 
    backgrounds = pipeBase.connectionTypes.Input(doc="Background model for the exposure",
                                                 storageClass="Background",
                                                 dimensions=("visit", "detector", "abstract_filter",
                                                             "skymap"),
                                                 name="calexpBackground",
                                                 multiple=True,
                                                 deferLoad=True)
    outputCatalog = pipeBase.connectionTypes.Output(doc="Aperture measurements",
                                                    dimensions=("visit", "detector", "abstract_filter",
                                                                "skymap"),
                                                    storageClass="SourceCatalog",
                                                    name="{outputName}")

    def __init__(self, *, config=None):
        super().__init__(config=config)
        
        if config.doLocalBackground is False:
            self.inputs.remove("background")


class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                         pipelineConnections=ApertureTaskConnections):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4) 
    doLocalBackground = pexConfig.Field(doc="Should the background be added before doing photometry",
                                        dtype=bool, default=False)


class ApertureTask(pipeBase.PipelineTask):

    ConfigClass = ApertureTaskConfig
    
    def __init__(self, config: pipeBase.Config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.apRad = self.config.apRad

        inputSchema = initInput['inputSchema'].schema
    
        # Create a camera mapper to create a copy of the input schema
        self.mapper = afwTable.SchemaMapper(inputSchema)
        self.mapper.addMinimalSchema(inputSchema, True)

        # Add our new field
        self.apKey = self.mapper.editOutputSchema().addField("apFlux", type=np.float64,
                                                             doc="Ap flux measured")

        # Get the output schema
        self.schema = mapper.getOutputSchema()
    
        # Create the output catalog
        self.outputCatalog = afwTable.SourceCatalog(self.schema)

        # Put the outputSchema into a SourceCatalog container. This var name matches an initOut
        # so will be persisted
        self.outputSchema = afwTable.SourceCatalog(self.schema)

    def run(self, exposures: typing.List[afwImage.Exposure],
            inputCatalogs: typing.List[afwTable.SourceCatalog],
            areaMasks: typing.List[afwImage.Mask],
            backgrounds: typing.Union[None, typing.List[afwMath.BackgroundList]] = None,) -> pipeBase.Struct:
        catalogLengths = []
        # Add in all the input catalogs into the output catalog
        for inCatHandle in inputCatalogs:
            inCat = inCatHandle.get()
            catalogLengths.append(len(inCat))
            self.outputCatalog.extend(inCat, mapper=self.mapper)

        # Track which input catalog a given source is coming from
        inputCatCount = 0
        cumLength = 0

        # Create some empty variables
        exposure, areaMask, dimensions, indy, indx = [None]*5

        # This function is an inner function (closure) and has access to variables defined outside
        # its scope. Based on what input catalog is being processed, it updates each of these
        # non local variables.
        def updateVars():
            nonlocal exposure, areaMask, dimensions, indy, indx, cumLength, inputCatCount
            exposure = exposure[inputCatCount].get()
            areaMask = areaMasks[inputCatCount].get()

            # If a background is supplied, add it back to the image so local background subtraction
            # can be done.
            if backgrounds is not none:
                exposure.image.array += backgrounds[inputCatCount].get().getImage()


            # Get the dimensions of the exposure
            dimensions = exposure.getDimensions()

            # Get indexes for each pixel
            indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

            cumLength += catalogLengths[inputCatCount]
            inputCatCount += 1
        
        updateVars()

        # Loop over each record in the catalog
        for i, source in enumerate(outputCatalog):
            # If the source from this catalog comes from the next input catalog in the list,
            # update all the inputs
            if i > cumLength:
                updateVars() 

            # Create an aperture and measure the flux
            center = source.getCentroid()

            # Skip measuring flux if the center of a source is in a masked pixel
            if areaMask.array[math.floor(center.getY()), math.floor(center.getX())] != 0:
                source.set(self.apKey, math.nan)
                continue

            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Do local background subtraction
            if background is not None:
                outerAn = distance < 2.5*self.apRad
                innerAn = distance < 1.5*self.apRad
                annulus = outerAn - innerAn
                localBackground = np.mean(exposure.image.array*annulus)
                flux -= np.sum(mask)*localBackground

            # Set the flux field of this source
            source.set(self.apKey, flux)
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

## Altering what is processed prior to processing

Our task is getting quite complicated, and so we are beginning to describe features of Pipeline tasks that
most tasks will not utilize. These are meant to describe what is available to you as a programmer, please
do not take it as a hammer and go looking for nails.

Our task is now doing photometry, optionally doing local background subtraction, and masking out
problematic areas over an entire focal plane. This leads to a potentially problematic situation. When the
activator system populates the lists of data-sets defined by our connection class, it will look for all
data-sets that match the specified dimensions of our task. It may occur that processing was done to produce
all the catalogs, backgrounds, and exposures, but someone failed to ingest a mask for some of the detectors
in a focal plane. Though this is not the only failure one can imagine, it is demonstrative of the issue at
hand. How should our task handle this kind of issue?

Connection classes have a method called `adjustQuantum` that is well suited for just this sort of thing.
This method gives a connection class an opportunity to inspect the quantum of work that is to be run, and
make any alterations it deems necessary prior to execution. This can be as simple as raising an exception
if something is missing, or as complicated an rewriting the quantum. Our task will do the latter, if any
detector position is laking a mask, all the other data-sets for that detector will be removed from the
quantum, and will not be processed. This is not the only way to handle this type of failure, but it ensures
what can be run is run. This situation is unique to our problem of processing an entire focal plain, had we
stuck with including a detector in our tasks dimensions, the execution system would have naturally done
this for us, but we would have had a far more boring demo. With this all in mind, lets look at our
connection class.

```python
class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              defaultTemplates={"outputName":"customAperture"},
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    ...

    def adjustQuantum(self, datasetRefMap):
        exposuresSet = {x.dataId for x in datasetRefMap.exposures}
        inputCatalogsSet = {x.dataId for x in datasetRefMap.inputCatalogs}
        backgroundsSet = {x.dataId for x in datasetRefMap.backgrounds}
        areaMasksSet = {x.dataId for x in datasetRefMap.areaMasks}

        commonDataIds = exposuresSet.intersect(inputCatalogsSet).intersect(backgroundsSet).\
                            intersect(areaMasksSet)

        for name, refs in datasetRefMap:
            tempList = [x for x in refs if x.dataId in commonDataIds]
            setattr(datasetRefMap, name, tempList)
        
        return datasetRefMap

```

The argument `datasetRefMap` in `adustQuantum` is a variable of type
`lsst.pipe.base.InputQuantizedConnection`. This data-structure has attributes with names corresponding to
the connections defined in the connections class. The value of these attributes are the
`lsst.daf.butler.DatasetRef`s that the execution system found associated with each connection. These
`lsst.daf.butler.DatasetRefs` are what will be loaded from the butler and supplied as arguments in the
execution of our task. We create sets out of the data ids corresponding to each of our inputs, and then
find the intersection of all the dataIds. This ensures that only ids that have all data products are to be
processed. This common set is then used to filter down the list associated with each of the input types.
The `datasetRefMap` is then returned where it will be used to generate a unit of processing to be executed.

The aggregated task can be seen below.

```python
import math
import typing

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.afw.table as afwTable
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath


class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              defaultTemplates={"outputName":"customAperture"},
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    inputSchema = pipeBase.connectionTypes.InitInput(doc="Schema associated with a src catalog",
                                                     storageClass="SourceCatalog",
                                                     name="src_schema")
    outputSchema = pipeBase.connectionTypes.InitOutput(doc="Schema generated in Aperture PipelineTask",
                                                       storageClass="SourceCatalog",
                                                       name="{outputName}_schema")
    areaMasks = pipeBase.connectionTypes.PrerequisiteInput(doc="A mask of areas to be ignored",
                                                           storageClass="Mask",
                                                           dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                                           name="ApAreaMask",
                                                           multiple=True,
                                                           deferLoad=True)
    exposures = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                               dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                               storageClass="ExposureF",
                                               name="calexp",
                                               multiple=True,
                                               deferLoad=True)
    inputCatalogs = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                   dimensions=("visit", "detector", "abstract_filter",
                                                               "skymap"),
                                                   storageClass="SourceCatalog",
                                                   name="src",
                                                   multiple=True,
                                                   deferLoad=True) 
    backgrounds = pipeBase.connectionTypes.Input(doc="Background model for the exposure",
                                                 storageClass="Background",
                                                 dimensions=("visit", "detector", "abstract_filter",
                                                            "skymap"),
                                                 name="calexpBackground",
                                                 multiple=True,
                                                 deferLoad=True)
    outputCatalog = pipeBase.connectionTypes.Output(doc="Aperture measurements",
                                                    dimensions=("visit", "detector", "abstract_filter",
                                                                "skymap"),
                                                    storageClass="SourceCatalog",
                                                    name="{outputName}")

    def __init__(self, *, config=None):
        super().__init__(config=config)
        
        if config.doLocalBackground is False:
            self.inputs.remove("background")

    def adjustQuantum(self, datasetRefMap):
        exposuresSet = {x.dataId for x in datasetRefMap.exposures}
        inputCatalogsSet = {x.dataId for x in datasetRefMap.inputCatalogs}
        backgroundsSet = {x.dataId for x in datasetRefMap.backgrounds}
        areaMasksSet = {x.dataId for x in datasetRefMap.areaMasks}

        commonDataIds = exposuresSet.intersect(inputCatalogsSet).intersect(backgroundsSet).\
                            intersect(areaMasksSet)

        for name, refs in datasetRefMap:
            tempList = [x for x in refs if x.dataId in commonDataIds]
            setattr(datasetRefMap, name, tempList)
        
        return datasetRefMap


class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                         pipelineConnections=ApertureTaskConnections):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4) 
    doLocalBackground = pexConfig.Field(doc="Should the background be added before doing photometry",
                                        dtype=bool, default=False)


class ApertureTask(pipeBase.PipelineTask):

    ConfigClass = ApertureTaskConfig
    
    def __init__(self, config: pipeBase.Config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.apRad = self.config.apRad

        inputSchema = initInput['inputSchema'].schema
    
        # Create a camera mapper to create a copy of the input schema
        self.mapper = afwTable.SchemaMapper(inputSchema)
        self.mapper.addMinimalSchema(inputSchema, True)

        # Add our new field
        self.apKey = self.mapper.editOutputSchema().addField("apFlux", type=np.float64,
                                                            doc="Ap flux measured")

        # Get the output schema
        self.schema = mapper.getOutputSchema()
    
        # Create the output catalog
        self.outputCatalog = afwTable.SourceCatalog(self.schema)

        # Put the outputSchema into a SourceCatalog container. This var name matches an initOut
        # so will be persisted
        self.outputSchema = afwTable.SourceCatalog(self.schema)

    def run(self, exposures: typing.List[afwImage.Exposure],
            inputCatalogs: typing.List[afwTable.SourceCatalog],
            areaMasks: typing.List[afwImage.Mask],
            backgrounds: typing.Union[None, typing.List[afwMath.BackgroundList]] = None,) -> pipeBase.Struct:
        catalogLengths = []
        # Add in all the input catalogs into the output catalog
        for inCatHandle in inputCatalogs:
            inCat = inCatHandle.get()
            catalogLengths.append(len(inCat))
            self.outputCatalog.extend(inCat, mapper=self.mapper)

        # Track which input catalog a given source is coming from
        inputCatCount = 0
        cumLength = 0

        # Create some empty variables
        exposure, areaMask, dimensions, indy, indx = [None]*5

        # This function is an inner function (closure) and has access to variables defined outside
        # its scope. Based on what input catalog is being processed, it updates each of these
        # non local variables.
        def updateVars():
            nonlocal exposure, areaMask, dimensions, indy, indx, cumLength, inputCatCount
            exposure = exposure[inputCatCount].get()
            areaMask = areaMasks[inputCatCount].get()

            # If a background is supplied, add it back to the image so local background subtraction
            # can be done.
            if backgrounds is not none:
                exposure.image.array += backgrounds[inputCatCount].get().getImage()


            # Get the dimensions of the exposure
            dimensions = exposure.getDimensions()

            # Get indexes for each pixel
            indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

            cumLength += catalogLengths[inputCatCount]
            inputCatCount += 1
        
        updateVars()

        # Loop over each record in the catalog
        for i, source in enumerate(outputCatalog):
            # If the source from this catalog comes from the next input catalog in the list,
            # update all the inputs
            if i > cumLength:
                updateVars() 

            # Create an aperture and measure the flux
            center = source.getCentroid()

            # Skip measuring flux if the center of a source is in a masked pixel
            if areaMask.array[math.floor(center.getY()), math.floor(center.getX())] != 0:
                source.set(self.apKey, math.nan)
                continue

            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Do local background subtraction
            if background is not None:
                outerAn = distance < 2.5*self.apRad
                innerAn = distance < 1.5*self.apRad
                annulus = outerAn - innerAn
                localBackground = np.mean(exposure.image.array*annulus)
                flux -= np.sum(mask)*localBackground

            # Set the flux field of this source
            source.set(self.apKey, flux)
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

## Overriding Task execution

Another advanced tool is overriding the `PipelineTask` method that is used in task execution, `runQuantum`.
This method is supplied identifiers for input dataset references to be used in processing, and output
dataset references for data-sets the activation frame work expects to be written at the end of processing.
The `runQuantum` method is responsible for fetching inputs, calling the run method, and writing outputs
using the supplied data ids. Let's go over the default implementation of `runQuantum`, and talk about
variations that will all accomplish the same thing, but in different ways. This will hopefully give some
introduction to what is possible with `runQuantum` and some ideas as to why you many need to override it.

```python
class PipelineTask(Task):
    ...

    def runQuantum(self, butlerQC: ButlerQuantumContext, inputRefs: InputQuantizedConnection,
                   outputRefs: OutputQuantizedConnection):
        inputs = butlerQC.get(inputRefs)
        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)
    
    ...
```

If this looks pretty strait forward to you, then great! This function is tightly packed, but short to make
the barrier to overloading it as low as possible.

Let's break down what we are looking at, starting with the arguments of `runQuantum`. The first argument is
named `butlerQC` which may cause you to wonder how it relates to a butler. Those of you that are paying
attention might know from the type annotation that the QC stands for QuantumContext, but that does not
really give many clues does it? A `ButlerQuantumContext` object is simply a butler that has special
information and functionality attached to to it about the unit of data your task will be processing, i.e.
the quantum. The next two arguments, `inputRefs` and `outputRefs`, supply `lsst.daf.butler.DatasetRef`s
that can be used to interact with specific data-sets managed by the butler. See the description of the
`InputQuantizedConnection` type in the section [Altering what is processed prior to processing](#altering-what-is-processed-prior-to-processing) for more
information on `QuantizedConnections`, noting that `OutputQuantizedConnection` functions in the same manor
but with output `lsst.daf.butler.DatasetRef`s.

The `runQuantum` method uses the `butlerQC` object has a `get` method that knows how understand the
structure of an `InputQuantizedConnection` and load in all of the inputs supplied in the inputRefs.
However, the `get` method also knows how to understand `List`s or single instances of
`lsst.dab.butler.DatasetRef`s. The butlerQC object also has a `put` method that mirrors all the
capabilities of the get method, put for putting outputs into the butler.

For examples sake lets add a `runQuantum` method to our photometry task that loads in all the input
references one connection at a time. Our task only expects to write a single data-set out, so our
`runQuantum` will also put with that single `lsst.daf.butler.DatasetRef`.

```python
class ApertureTask(pipeBase.PipelineTask):
    ...

    def runQuantum(self, butlerQC: pipeBase.ButlerQuantumContext,
                   inputRefs: pipeBase.InputQuantizedConnection,
                   outputRefs: pipeBase.OutputQuantizedConnection):
        inputs = {}
        for name, refs in inputRefs:
            inputs[name] = butlerQC.get(refs)
        output = self.run(**inputs)
        butlerQC.put(output, outputRefs.OutputCatalog)
```

Overriding `runQuantum` also provides the opportunity to do a transformation on input data, or some other
related calculation. This allows the `run` method to have a convenient interface for user interaction
within a notebook or shell, but still match the types of input `PipelineTask` activators will supply. To
demonstrate this, lets modify the `runQuantum` and `run` methods in such a way that the output catalog of
the task is already pre-populated with all of the input catalogs. The user then only needs to supply the
lengths of each of the input catalogs that went in to creating the output catalog. This change is likely
not worth doing in a production `PipelineTask` but is perfect for demoing the concepts here.

```python
    ...

    def runQuantum(self, butlerQC: pipeBase.ButlerQuantumContext,
                   inputRefs: pipeBase.InputQuantizedConnection,
                   outputRefs: pipeBase.OutputQuantizedConnection):
        inputs = {}
        for name, refs in inputRefs:
            inputs[name] = butlerQC.get(refs)
        
        # Record the lengths of each input catalog
        catalogLengths = []

        # Remove the input catalogs from the list of inputs to the run method
        inputCatalogs = inputs.pop('inputCatalogs')
        # Add in all the input catalogs into the output catalog
        for inCatHandle in inputCatalogs:
            inCat = inCatHandle.get()
            catalogLengths.append(len(inCat))
            self.outputCatalog.extend(inCat, mapper=self.mapper)

        # Add the catalog lengths to the inputs to the run method
        inputs['catalogLengths'] = catalogLengths
        output = self.run(**inputs)
        butlerQC.put(output, outputRefs.OutputCatalog)
    
    def run(self, exposures: typing.List[afwImage.Exposure], catalogLengths: typing.List[int],
            areaMasks: typing.List[afwImage.Mask],
            backgrounds: typing.Union[None, typing.List[afwMath.BackgroundList]] = None,) -> pipeBase.Struct:

        # Track which input catalog a given source is coming from
        inputCatCount = 0
        cumLength = 0

        ...
```

Adding additional logic to a `PipelineTask` in this way is very powerful, but should be using sparingly and
with great thought. Putting logic in `runQuantum` not only makes it more difficult to follow the flow of
the algorithm, but creates the need for duplication of that logic in contexts where the `run` method is
called outside of `PipelineTask` execution.

Our example task with `runQuantum` now looks like this.

```python
import math
import typing

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.afw.table as afwTable
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath


class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              defaultTemplates={"outputName":"customAperture"},
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    inputSchema = pipeBase.connectionTypes.InitInput(doc="Schema associated with a src catalog",
                                                     storageClass="SourceCatalog",
                                                     name="src_schema")
    outputSchema = pipeBase.connectionTypes.InitOutput(doc="Schema generated in Aperture PipelineTask",
                                                       storageClass="SourceCatalog",
                                                       name="{outputName}_schema")
    areaMasks = pipeBase.connectionTypes.PrerequisiteInput(doc="A mask of areas to be ignored",
                                                           storageClass="Mask",
                                                           dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                                           name="ApAreaMask",
                                                           multiple=True,
                                                           deferLoad=True)
    exposures = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                               dimensions=("visit", "detector", "abstract_filter", "skymap"),
                                               storageClass="ExposureF",
                                               name="calexp",
                                               multiple=True,
                                               deferLoad=True)
    inputCatalogs = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                   dimensions=("visit", "detector", "abstract_filter",
                                                               "skymap"),
                                                   storageClass="SourceCatalog",
                                                   name="src",
                                                   multiple=True,
                                                   deferLoad=True) 
    backgrounds = pipeBase.connectionTypes.Input(doc="Background model for the exposure",
                                                 storageClass="Background",
                                                 dimensions=("visit", "detector", "abstract_filter",
                                                             "skymap"),
                                                 name="calexpBackground",
                                                 multiple=True,
                                                 deferLoad=True)
    outputCatalog = pipeBase.connectionTypes.Output(doc="Aperture measurements",
                                                    dimensions=("visit", "detector", "abstract_filter",
                                                                "skymap"),
                                                    storageClass="SourceCatalog",
                                                    name="{outputName}")

    def __init__(self, *, config=None):
        super().__init__(config=config)
        
        if config.doLocalBackground is False:
            self.inputs.remove("background")

    def adjustQuantum(self, datasetRefMap):
        exposuresSet = {x.dataId for x in datasetRefMap.exposures}
        inputCatalogsSet = {x.dataId for x in datasetRefMap.inputCatalogs}
        backgroundsSet = {x.dataId for x in datasetRefMap.backgrounds}
        areaMasksSet = {x.dataId for x in datasetRefMap.areaMasks}

        commonDataIds = exposuresSet.intersect(inputCatalogsSet).intersect(backgroundsSet).\
                            intersect(areaMasksSet)

        for name, refs in datasetRefMap:
            tempList = [x for x in refs if x.dataId in commonDataIds]
            setattr(datasetRefMap, name, tempList)
        
        return datasetRefMap


class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                         pipelineConnections=ApertureTaskConnections):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4) 
    doLocalBackground = pexConfig.Field(doc="Should the background be added before doing photometry",
                                        dtype=bool, default=False)


class ApertureTask(pipeBase.PipelineTask):

    ConfigClass = ApertureTaskConfig
    
    def __init__(self, config: pipeBase.Config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.apRad = self.config.apRad

        inputSchema = initInput['inputSchema'].schema
    
        # Create a camera mapper to create a copy of the input schema
        self.mapper = afwTable.SchemaMapper(inputSchema)
        self.mapper.addMinimalSchema(inputSchema, True)

        # Add our new field
        self.apKey = self.mapper.editOutputSchema().addField("apFlux", type=np.float64,
                                                             doc="Ap flux measured")

        # Get the output schema
        self.schema = mapper.getOutputSchema()
    
        # Create the output catalog
        self.outputCatalog = afwTable.SourceCatalog(self.schema)

        # Put the outputSchema into a SourceCatalog container. This var name matches an initOut
        # so will be persisted
        self.outputSchema = afwTable.SourceCatalog(self.schema)

    def runQuantum(self, butlerQC: pipeBase.ButlerQuantumContext,
                   inputRefs: pipeBase.InputQuantizedConnection,
                   outputRefs: pipeBase.OutputQuantizedConnection):
        inputs = {}
        for name, refs in inputRefs:
            inputs[name] = butlerQC.get(refs)
        
        # Record the lengths of each input catalog
        catalogLengths = []

        # Remove the input catalogs from the list of inputs to the run method
        inputCatalogs = inputs.pop('inputCatalogs')
        # Add in all the input catalogs into the output catalog
        for inCatHandle in inputCatalogs:
            inCat = inCatHandle.get()
            catalogLengths.append(len(inCat))
            self.outputCatalog.extend(inCat, mapper=self.mapper)

        # Add the catalog lengths to the inputs to the run method
        inputs['catalogLengths'] = catalogLengths
        output = self.run(**inputs)
        butlerQC.put(output, outputRefs.OutputCatalog)

    def run(self, exposures: typing.List[afwImage.Exposure], catalogLengths: typing.List[int],
            areaMasks: typing.List[afwImage.Mask],
            backgrounds: typing.Union[None, typing.List[afwMath.BackgroundList]] = None,) -> pipeBase.Struct:

        # Track which input catalog a given source is coming from
        inputCatCount = 0
        cumLength = 0

        # Create some empty variables
        exposure, areaMask, dimensions, indy, indx = [None]*5

        # This function is an inner function (closure) and has access to variables defined outside
        # its scope. Based on what input catalog is being processed, it updates each of these
        # non local variables.
        def updateVars():
            nonlocal exposure, areaMask, dimensions, indy, indx, cumLength, inputCatCount
            exposure = exposure[inputCatCount].get()
            areaMask = areaMasks[inputCatCount].get()

            # If a background is supplied, add it back to the image so local background subtraction
            # can be done.
            if backgrounds is not none:
                exposure.image.array += backgrounds[inputCatCount].get().getImage()


            # Get the dimensions of the exposure
            dimensions = exposure.getDimensions()

            # Get indexes for each pixel
            indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

            cumLength += catalogLengths[inputCatCount]
            inputCatCount += 1
        
        updateVars()

        # Loop over each record in the catalog
        for i, source in enumerate(outputCatalog):
            # If the source from this catalog comes from the next input catalog in the list,
            # update all the inputs
            if i > cumLength:
                updateVars() 

            # Create an aperture and measure the flux
            center = source.getCentroid()

            # Skip measuring flux if the center of a source is in a masked pixel
            if areaMask.array[math.floor(center.getY()), math.floor(center.getX())] != 0:
                source.set(self.apKey, math.nan)
                continue

            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Do local background subtraction
            if background is not None:
                outerAn = distance < 2.5*self.apRad
                innerAn = distance < 1.5*self.apRad
                annulus = outerAn - innerAn
                localBackground = np.mean(exposure.image.array*annulus)
                flux -= np.sum(mask)*localBackground

            # Set the flux field of this source
            source.set(self.apKey, flux)
        
        return pipeBase.Struct(outputCatalog=self.outputCatalogs)
```

## Glossary 

### Activator
An activator is a piece of code designed to execute one or more `PipelineTasks`. They are responsible for
determining what data is to be run from the defined connections, quantum, and what exists inside a butler
registry.

### Registry
A registry is the area of the butler that keeps track of information about data-sets. This information
includes what data-sets exist, their dimensions, and relationships between data-sets.

### Dimension
A dimension is an attribute of a data-set that is tracked by the butler, and used when referring to the
data-set. I.e. an image has a detector dimension corresponding to the individual ccd that took the image.

### Quantum
The combination of dimensions that describes the unit of work for a `PipelineTask`.

### Storage Classes
A name used by the butler to identify what transformation to make between python objects, and storage
formats. The base storage classes are defined in `daf_butler/config/storageCLasses.yaml`, with optional
user defined types possible. See `daf_butler` documentation for more details.