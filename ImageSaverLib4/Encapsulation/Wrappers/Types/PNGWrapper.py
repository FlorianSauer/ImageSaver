import io
import math
import struct
from typing import Tuple

import numpy
from PIL import Image

from ..BaseWrapper import BaseWrapper
from ..WrapperErrors import UnWrapError


class PNGWrapper(BaseWrapper):
    """
    wrapper for PNG images
    hides a given payload fully in a PNG image
    the first pixel stores the payload size
    after the payload a variable number of color vectors will get added, so the resulting PNG-Image has a rectange shape

    recommended extension: 'png'

    CAUTION: this wrapper does not perform any sort of image stenography (hiding payload inside of an image, without
    manipulating the visual image). All given payload will get stored 'as-is' as a (rectangular) png image (with a
    little bit of head- and tail-padding)

    """

    _wrapper_type = 'png'

    _int_struct = struct.Struct('!I')
    _int_struct_len = 4

    @classmethod
    def calcMinimumPadding(cls, data):
        # type: (bytes) -> int
        """
        Calculate the minimum padding, to fill up a 4D-Vector

        for shape (x, x, 4)
        """
        return int(math.ceil(len(data) / 4) * 4) - len(data)

    @classmethod
    def addSizeVectorPadding(cls, data):
        # type: (bytes) -> bytes
        tail_padding = cls.calcMinimumPadding(data)
        return cls._int_struct.pack(len(data)) + data + bytes(tail_padding)

    @classmethod
    def calcRectangularImagePadding(cls, pre_padded_data):
        # type: (bytes) -> int
        """
        Calculate the tail-padding for already pre-padded data (size header, payload, zero-padding), so that the whole
        pre-padded data + image padding will result in a rectangular image.

        :return: the amount of padding color vectors
        """
        # assert len(pre_padded_data) % 4 == 0, "pre_padded_data is not multiple of 4"
        if len(pre_padded_data) % 4 != 0:
            raise ValueError("pre_padded_data is not multiple of 4")
        color_vectors_count = len(pre_padded_data) / 4
        # print("color_vectors_count", color_vectors_count)
        if color_vectors_count == 1:
            # print("pre_padded_data only contains a single color vector")
            return 0
        if math.ceil(math.sqrt(color_vectors_count)) ** 2 == color_vectors_count:
            # print("pre_padded_data is rectangular, no padding needed")
            return 0

        # print("color vector count for next square", math.ceil(math.sqrt(color_vectors_count)))
        # print("padding color vectors", (math.ceil(math.sqrt(color_vectors_count)) ** 2) - color_vectors_count)
        return int((math.ceil(math.sqrt(color_vectors_count)) ** 2) - color_vectors_count)

    @classmethod
    def addRectangularColorVectorPadding(cls, data):
        # type: (bytes) -> bytes
        missing_color_vectors = cls.calcRectangularImagePadding(data)
        return data + bytes(missing_color_vectors * 4)

    @classmethod
    def addPaddings(cls, data):
        # type: (bytes) -> bytes
        """
        adds all paddings to the given payload
        [<4 bytes size header><Payload><vector-padding>][Color-vector-padding]

        - vector padding is the padding which is required to 'fill up' a vector to a 4D-vector
        - Color vector padding is the padding which is required to 'fill up' the image to a valid size (Custom or
          Rectangular)
        """
        # print("payload size", len(data))
        size_vector_padded = cls.addSizeVectorPadding(data)
        # print("size-vector padded data size", len(size_vector_padded))
        color_vector_padded = cls.addRectangularColorVectorPadding(size_vector_padded)
        # print("color-vector padded data size", len(color_vector_padded))
        return color_vector_padded

    @classmethod
    def stripPadding(cls, padded_data):
        # type: (bytes) -> bytes
        if len(padded_data) < 4:
            raise ValueError("given padded data is not long enough to store the size header (minimum 4 bytes)")
        size = cls._int_struct.unpack(padded_data[:cls._int_struct_len])[0]
        payload = padded_data[cls._int_struct_len:cls._int_struct_len + size]
        if len(payload) != size:
            raise UnWrapError("payload was not as long as stated in the first pixel/4-bytes")
        return payload

    @classmethod
    def calcShapeFromPadding(cls, padded_data):
        # type: (bytes) -> Tuple[int, int, int]
        color_vectors_count = len(padded_data) / 4
        x_y_axis_len = math.ceil(math.sqrt(color_vectors_count))
        return x_y_axis_len, x_y_axis_len, 4

    @classmethod
    def wrap(cls, data):
        padded_data = cls.addPaddings(data)
        shape = cls.calcShapeFromPadding(padded_data)
        # print("wrapped to shape", shape, "resulting in", functools.reduce(lambda x, y: x * y, shape), "bytes with",
        #       len(data), "bytes of payload")
        flat_arr = numpy.frombuffer(padded_data, dtype='uint8')
        # vector = numpy.array(flat_arr)
        # arr2 = numpy.asarray(vector).reshape(shape)
        arr2 = numpy.asarray(flat_arr).reshape(shape)
        img2 = Image.fromarray(arr2, 'RGBA')
        imgByteArr = io.BytesIO()
        img2.save(imgByteArr, format='PNG')
        return imgByteArr.getvalue()

    @classmethod
    def unwrap(cls, data):
        imgByteArr = io.BytesIO(data)
        img = Image.open(imgByteArr).convert('RGBA')
        arr = numpy.array(img).ravel()
        image_bytes = arr.tobytes()
        return cls.stripPadding(image_bytes)

class PNG3DWrapper(BaseWrapper):
    """
    3D VECTOR VARIANT - more google friendly

    wrapper for PNG images
    hides a given payload fully in a PNG image
    the first pixel stores the payload size
    after the payload a variable number of color vectors will get added, so the resulting PNG-Image has a rectange shape

    recommended extension: 'png'

    CAUTION: this wrapper does not perform any sort of image stenography (hiding payload inside of an image, without
    manipulating the visual image). All given payload will get stored 'as-is' as a (rectangular) png image (with a
    little bit of head- and tail-padding)

    """

    _wrapper_type = 'png3d'

    _int_struct = struct.Struct('!I')
    _int_struct_len = 4

    @classmethod
    def calcMinimumPadding(cls, data):
        # type: (bytes) -> int
        """
        Calculate the minimum padding, to fill up a 3D-Vector

        for shape (x, x, 3)
        """
        return int(math.ceil(len(data) / 3) * 3) - len(data)

    @classmethod
    def addSizeVectorPadding(cls, data):
        # type: (bytes) -> bytes
        tail_padding = cls.calcMinimumPadding(data) + 2  # +2 because length pixel needs 4 bytes
        return cls._int_struct.pack(len(data)) + data + bytes(tail_padding)

    @classmethod
    def calcRectangularImagePadding(cls, pre_padded_data):
        # type: (bytes) -> int
        """
        Calculate the tail-padding for already pre-padded data (size header, payload, zero-padding), so that the whole
        pre-padded data + image padding will result in a rectangular image.

        :return: the amount of padding color vectors
        """
        # assert len(pre_padded_data) % 4 == 0, "pre_padded_data is not multiple of 4"
        if len(pre_padded_data) % 3 != 0:
            raise ValueError("pre_padded_data is not multiple of 3")
        color_vectors_count = len(pre_padded_data) / 3
        # print("color_vectors_count", color_vectors_count)
        if color_vectors_count == 1:
            # print("pre_padded_data only contains a single color vector")
            return 0
        if math.ceil(math.sqrt(color_vectors_count)) ** 2 == color_vectors_count:
            # print("pre_padded_data is rectangular, no padding needed")
            return 0

        # print("color vector count for next square", math.ceil(math.sqrt(color_vectors_count)))
        # print("padding color vectors", (math.ceil(math.sqrt(color_vectors_count)) ** 2) - color_vectors_count)
        return int((math.ceil(math.sqrt(color_vectors_count)) ** 2) - color_vectors_count)

    @classmethod
    def addRectangularColorVectorPadding(cls, data):
        # type: (bytes) -> bytes
        missing_color_vectors = cls.calcRectangularImagePadding(data)
        return data + bytes(missing_color_vectors * 3)

    @classmethod
    def addPaddings(cls, data):
        # type: (bytes) -> bytes
        """
        adds all paddings to the given payload
        [<4 bytes size header><Payload><vector-padding>][Color-vector-padding]

        - vector padding is the padding which is required to 'fill up' a vector to a 4D-vector
        - Color vector padding is the padding which is required to 'fill up' the image to a valid size (Custom or
          Rectangular)
        """
        # print("payload size", len(data))
        size_vector_padded = cls.addSizeVectorPadding(data)
        # print("size-vector padded data size", len(size_vector_padded))
        color_vector_padded = cls.addRectangularColorVectorPadding(size_vector_padded)
        # print("color-vector padded data size", len(color_vector_padded))
        return color_vector_padded

    @classmethod
    def stripPadding(cls, padded_data):
        # type: (bytes) -> bytes
        if len(padded_data) < 3:
            raise ValueError("given padded data is not long enough to store the size header (minimum 3 bytes)")
        size = cls._int_struct.unpack(padded_data[:cls._int_struct_len])[0]
        payload = padded_data[cls._int_struct_len:cls._int_struct_len + size]
        if len(payload) != size:
            raise UnWrapError("payload was not as long as stated in the first pixel/3-bytes")
        return payload

    @classmethod
    def calcShapeFromPadding(cls, padded_data):
        # type: (bytes) -> Tuple[int, int, int]
        color_vectors_count = len(padded_data) / 3
        x_y_axis_len = math.ceil(math.sqrt(color_vectors_count))
        return x_y_axis_len, x_y_axis_len, 3

    @classmethod
    def wrap(cls, data):
        padded_data = cls.addPaddings(data)
        shape = cls.calcShapeFromPadding(padded_data)
        # print("wrapped to shape", shape, "resulting in", functools.reduce(lambda x, y: x * y, shape), "bytes with",
        #       len(data), "bytes of payload")
        flat_arr = numpy.frombuffer(padded_data, dtype='uint8')
        # vector = numpy.array(flat_arr)
        # arr2 = numpy.asarray(vector).reshape(shape)
        arr2 = numpy.asarray(flat_arr).reshape(shape)
        img2 = Image.fromarray(arr2, 'RGB')
        imgByteArr = io.BytesIO()
        img2.save(imgByteArr, format='PNG')
        return imgByteArr.getvalue()

    @classmethod
    def unwrap(cls, data):
        imgByteArr = io.BytesIO(data)
        img = Image.open(imgByteArr).convert('RGB')
        arr = numpy.array(img).ravel()
        image_bytes = arr.tobytes()
        return cls.stripPadding(image_bytes)

