from ..BaseWrapper import BaseWrapper
from ..WrapperErrors import UnWrapError


class SVGWrapper(BaseWrapper):

    _wrapper_type = 'svg'
    pre_data = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.0//EN" "http://www.w3.org/TR/2001/PR-SVG-20010719/DTD/svg10.dtd">
<svg width="5cm" height="2cm" viewBox="125 134 83 39" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
  <g>
    <rect style="fill: #ffffff" x="126" y="135" width="80" height="36" rx="10" ry="10"/>
    <rect style="fill: none; fill-opacity:0; stroke-width: 2; stroke: #000000" x="126" y="135" width="80" height="36" rx="10" ry="10"/>
    <text font-size="12.7998" style="fill: #000000;text-anchor:middle;font-family:sans-serif;font-style:normal;font-weight:normal" x="166" y="156.9">
      <tspan x="166" y="156.9">"""

    post_data = """</tspan>
    </text>
  </g>
</svg>"""

    @classmethod
    def wrap(cls, data):
        return (cls.pre_data+data.hex()+cls.post_data).encode('utf-8')

    @classmethod
    def unwrap(cls, data):
        s = data.decode('utf-8')
        if not s.startswith(cls.pre_data):
            raise UnWrapError("unable to decompress data, data does not start with expected svg start data")
        if not s.endswith(cls.post_data):
            raise UnWrapError("unable to decompress data, data does not end with expected svg end data")
        s = s.replace(cls.pre_data, '', 1)  # remove pre_data
        s = ''.join(s.rsplit(cls.post_data, 1))  # remove post_data
        return bytes.fromhex(s)
