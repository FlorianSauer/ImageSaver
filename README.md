# ImageSaver

ImageSaver is a Tool for saving binary Data on Services, which normally do not support this kind of data.

ImageSaver was developed to upload any Files to services like Flickr or Google Photos, which provide 1TB (Flickr) or unlimited (Google Photos) Storage Space.


### When is a Service a supported Service?

ImageSaver currently supports: 
- In-Memory Storage
- File System Storage
- Samba Storage
- Dropbox Storage
- Google Photos Storage

Basic requirements for a Storage Service are
- Uploaded data must be retrievable with a unique name (Filename, URL, Hash, etc...)
- Uploading data must generate a unique String, with which ImageSaver can re-download it
- Uploaded data must not be manipulated or changed
- All Names of uploaded Resources must be listable
- Uploaded data must be deletable by its unique name


### Usage and internal Workflow
ImageSaver tries its best to work with streams, however at some point some data has to be stored in caches to reduce the usage of the target Service.

If you pass a big text file to ImageSaver it will treat your file as a Compound, which consists of Fragments, which are stored in Resources.

##### Basic Layers overview:
1. Compound Layer
2. 1st Encapsulation Layer
3. Fragment Layer
4. 2nd Encapsulation Layer
5. Resource Layer

##### Basic Layers workflow description
- To generate the Fragments, the file/stream gets split up into chunks.
- Before building the Fragment, the chunks are encapsulated (first they get compressed, then wrapped up). A Fragment is therefore a compressed and wrapped chunk of a byte stream.
- The Fragments are passed on to a Fragment Cache, which stores the fragments in-memory until enough fragments are collected to build a Resource.
- Before building the Resource, the Fragments are encapsulated (first they get compressed, then wrapped up).
- The Resource then gets uploaded to a Storage-Service and optionally added to a Resource Cache, to skip download times.
- Keep in mind, that the 2nd Encapsulation Layer, which creates the Resource, has to create files, which are supported by the Service. This can be done by choosing a matching wrapper.

##### Compound Layer and 1st Encapsulation Layer
The Compound Layer only stores the the Name of a given stream, the hash and size of the total stream and how the Compound was encapsulated.

##### Fragment Layer
The Fragment Layer only stores the hashes and sizes of stream-chunks.
A Fragment can be used multiple times by Compounds.
Keep in mind that this mechanism only works/works best, if the Fragment size is equal for all Compounds.

Warning: ImageSaver does not detect random patterns of different sizes. Only patterns of fixed size are detected, by splitting the stream into Fragments and comparing the hashes of these Fragments.

##### 2nd Encapsulation Layer and Resource layer
The Resource Layer is the Layer which stores and uploads your packed up data on a target Service.
To do this, the 2nd Encapsulation Layer has to wrap up the Fragments in a file format, which is supported by the Service.
During creation of a Resource, multiple Fragments can be packed into a Resource, to use the service more efficiently.
This comes in handy, if the Service only allows a specific number of files.

Keep in mind, that the Service should not manipulate the uploaded Resource in any way, or use a Wrapper which takes this into account.


##### Alternative Layers overview
1. Named Stream Layer, 
This Layer is a basic mapping of a Keyword to a given Stream. 
Additionally the given stream is either wrapped or compressed.
It is responsible to create mappings between a Keyword and the steam (or better its hash) and mappings between the created Fragments from the underlying Layer with their sequence and the stream
2. Fragment Layer, 
This Layer splits the received stream into Fragments and passes them to the 3rd Layer. 
It is responsible to filter duplicate Fragments and map the Fragments to a Resource from the 3rd Layer.
3. Resource Layer, This Layer packs the received Fragments together and uploads them to a Storage Service. It is responsible to compress and most importantly wrap the Fragments in a supported File format (PNG).  
4. Storage Service Layer, this Layer is basically the Serivce which is used to store the data. This is not a part of ImageSaver.


### Recommendations
#### Wrappers for 2nd Encapsulation Layer
It is recommended, to use the PNG wrapper for this encapsulation layer.
PNGs use lossless compression and data can be stored without manipulation.
However the Service should also not manipulatie the PNGs in any way.

An alternative is the SVG wrapper, however this wrapper doubles the needed space, because stored data has to be hex-encoded.

To use Resource Space as efficiently as possible, you should also only use a Compresser in the first Encapsulation Layer.
This will shrink down Fragment sizes early and allows to pack more Fragments into Resources.
Compressing Resources is more difficult to control.

#### Fragment and Resource Sizes; How can I use my Storage Service without wasting Space
If your Service has limits for maximum file sizes and file count, you are more or less
forced to use the space in the most optimal way.
This is nearly impossible without a lot of tweaking.
You can optimize your space usage with:
- Resource Size
- Fragment Size
- Compression
- Wrapping

##### Tweaking Resource Size
The most optimal Resource Size is the maximum allowed file size.
If your Service supports binary Files (without 2nd encapsulation Layer Wrapping) you are lucky.
If you want to upload BIG files, use a high Fragment size near the calculated Resource Size, add a good Compression in the 1st encapsulation layer and substract the compression overhead size.
This way a fragment will fit near perfectly into a Resource.
If you want to upload LOTS of small files, it could be best to pack them up into a .zip file and then upload this one large file with ImageSaver, by using the previous method.
If the .zip packing is not an option, choosing a fragment size becomes an experiment.
You have to take into account, that the selected fragment size and the resulting compression overhead summed together must fill a Resource without wasting much bytes.

If your Service does not support binary files and only accepts images (PNGs) things get even more complicated.
You additional have to take into account, that the PNG-Wrapping adds additional overhead, which increases the Resource Size, but also can compress some fragments.
Then you either end up with a Resource which is some bytes too large to fit into your Service or the compression of the PNG-wrapper is good enough, that you end up with wasted space.

A rule of thumb would be to use a big Fragment Size for big files and substract some bytes for compression overhead.
If you then upload small files, use a small Fragment Size to 'fill up' the resources.
