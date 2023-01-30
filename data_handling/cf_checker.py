import xml.etree.ElementTree as ET
import json
import logging
import xarray as xr


class CFChecker:
    """
    TODO: finish this docstrings
    https://cfconventions.org/vocabularies.html
    """

    def __init__(self, cf_dict: dict or None = None) -> None:
        self._cf_dict = cf_dict

    @staticmethod
    def _create_two_way_dict(dict_to_extend: dict) -> dict:
        """
        Creates a dummy two-day dictionary by simply inverting a dictionary and merging the inverted with the original.

        Parameters
        ----------
        dict_to_extend
            Dictionary from which the dummy two-way dictionary will be created.

        Returns
        -------
        dict_to_extend
            Dummy two-way dictionary

        """
        tmp_dict = {}
        for key, val in dict_to_extend.items():
            tmp_dict[val] = key

        # Merge dictionaries
        dict_to_extend = dict_to_extend | tmp_dict

        return dict_to_extend

    def xml_to_dict(self, path: str) -> dict:
        """
        Creates a dictionary of the CF standard names table from its XMl file.

        Parameters
        ----------
        path
            Absolute or relative filepath of the XML file.

        Returns
        -------
        cf_dict
            Dictionary of the CF table.
        """
        tree = ET.parse(path)
        root = tree.getroot()

        self._cf_dict = {}
        for entry in root.iter("entry"):
            id = entry.attrib["id"]
            self._cf_dict[id] = {}
            for attr in root.findall(f"./entry[@id='{id}']/*"):
                self._cf_dict[id][attr.tag] = attr.text

        return self._cf_dict

    def write_cf_table_to_json(self, path: str = "cf-standard-name-table.json"):
        """
        Write the CF standard names table to a JSON file.

        Parameters
        ----------
        path
            Absolute or relative filepath of the JSON file.

        Returns
        -------
        cf_dict
            Dictionary of the CF table.
        """
        with open(path, 'w') as f:
            f.write(json.dumps(self._cf_dict))

        return self._cf_dict

    def load_cf_table_from_json(self, path: str = "cf-standard-name-table.json") -> dict:
        """
        Loads the CF standard names table from a JSON file.

        Parameters
        ----------
        path
            Absolute or relative filepath to the JSON file.

        Returns
        -------
        cf_dict
            Dictionary of the CF table.
        """
        with open(path, 'r') as f:
            self._cf_dict = json.load(f)

        return self._cf_dict

    def repair_metadata(self, ds: xr.Dataset or xr.DataArray, name_mapping: dict = {}) -> xr.Dataset or xr.DataArray:
        """
        Checks and repairs the metadata of a xarray `Dataset` or `DataArray`.

        Currently, two types of checks are implemented:

        CF_NAMES:
        Attempts to set standard names equal to long names and vice-versa, if one of them is missing and the other is CF-compliant.
        Otherwise, attempts to set standard/long names to the correspondent names in the provided mapping.
        If no mapping was provided and there is no CF-complacency, sets one equal to the other.
        Warns if the final standard name is not CF-compliant.

        CF_UNITS:
        Checks if units are CF-compliant and warns if that is not the case.

        Parameters
        ----------
        ds:
            xarray `Dataset` or `DataArray`.
        name_mapping
            Dictionary that maps standard names into long names or vice-versa.
            A two-way dictionary is generated at the beginning of this method.

        Returns
        -------
        ds:
            Repaired xarray `Dataset` or `DataArray`.
        """
        name_mapping = self._create_two_way_dict(name_mapping)

        for var in ds.variables:
            var_attrs = ds[var].attrs

            if "standard_name" in var_attrs:
                if "long_name" in var_attrs:
                    if var_attrs["standard_name"] not in self._cf_dict:
                        logging.warning(f"CF_NAMES:Standard name of '{var}' "
                                        f"['{var_attrs['standard_name']}'] is not a CF standard.")
                else:
                    if var_attrs["standard_name"] not in self._cf_dict:
                        logging.warning(f"CF_NAMES:Standard name of '{var}' "
                                        f"['{var_attrs['standard_name']}'] is not a CF standard.")
                        if var_attrs["standard_name"] in name_mapping:
                            logging.warning(f"CF_NAMES:Long name of '{var}' ['{var_attrs['long_name']}'] will be set "
                                            f"using the mapping ['{name_mapping[var_attrs['standard_name']]}'].")
                            var_attrs["long_name"] = name_mapping[var_attrs["standard_name"]]
                        else:
                            logging.warning(f"CF_NAMES:Long name of '{var}' will be set using "
                                            f"its not CF-compliant standard name ['{var_attrs['standard_name']}'].")
                            var_attrs["long_name"] = var_attrs["standard_name"]
                    else:
                        var_attrs["long_name"] = var_attrs["standard_name"]
                        logging.info(f"CF_NAMES:Long name of '{var}' ['{var_attrs['long_name']}'] "
                                        f"will be set using its CF-compliant standard name ['{var_attrs['standard_name']}'].")
            elif "long_name" in var_attrs:
                logging.info(f"CF_NAMES:Standard name of '{var}' is not set.")
                if var_attrs["long_name"] not in self._cf_dict:
                    logging.info(f"CF_NAMES:Long name of '{var}' ['{var_attrs['long_name']}'] is not a CF standard.")
                    if var_attrs["long_name"] in name_mapping:
                        logging.warning(f"CF_NAMES:Standard name of '{var}' will be set "
                                        f"using the mapping ['{name_mapping[var_attrs['long_name']]}'].")
                        var_attrs["standard_name"] = name_mapping[var_attrs["long_name"]]
                    else:
                        logging.warning(
                            f"CF_NAMES:Standard name of '{var}' will be set using its "
                            f"not CF-compliant long name ['{var_attrs['long_name']}'].")
                        var_attrs["standard_name"] = var_attrs["long_name"]

                else:
                    logging.info(f"CF_NAMES:Standard name of '{var}' will be set using its "
                                    f"CF-compliant long name ['{var_attrs['long_name']}'].")
                    var_attrs["standard_name"] = var_attrs["long_name"]

            else:
                logging.warning(f"CF_NAMES:Standard and long name of '{var}' are not set.")

            # Check CF-compliance of the units
            if "units" in var_attrs:
                if var_attrs["standard_name"] in self._cf_dict:
                    if var_attrs["units"] == self._cf_dict[var_attrs["standard_name"]]["canonical_units"]:
                        logging.info(f"CF_UNITS:Units of '{var}' [{var_attrs['units']}] are CF-compliant"
                                        f" [{self._cf_dict[var_attrs['standard_name']]['canonical_units']}].")
                    else:
                        logging.warning(f"CF_UNITS:Units of '{var}' [{var_attrs['units']}] are not CF-compliant "
                                        f"[{self._cf_dict[var_attrs['standard_name']]['canonical_units']}].")
                else:
                    logging.warning(f"CF_UNITS:CF-compliance of '{var}' [{var_attrs['units']}] units is not possible "
                                    f"to check as it is does not have a CF-compliant standard name.")
            else:
                logging.warning(f"CF_UNITS:Units of '{var}' are not set.")

        return ds


if __name__ == "__main__":
    import urllib.request

    url = "https://cfconventions.org/Data/cf-standard-names/79/src/cf-standard-name-table.xml"
    xml_file, headers = urllib.request.urlretrieve(url)

    mapping = {"Vertical T levels": "depth",
               "sea_water_potential_temperature": "sea_water_potential_temperature",
               "sea_water_salinity": "sea_water_salinity",
               "sea_surface_salinity": "sea_surface_salinity",
               "sea_surface_temperature": "sea_surface_temperature",
               "square_of_sea_surface_temperature": "square_of_sea_surface_temperature",
               "sea_surface_height_above_geoid": "sea_surface_height_above_geoid",
               "square_of_sea_surface_height_above_geoid": "square_of_sea_surface_height_above_geoid",
               "water_flux_into_sea_water": "water_flux_into_sea_water",
               "surface_net_downward_shortwave_flux": "surface_net_downward_shortwave_flux",
               "surface_net_downward_total_heat_flux": "surface_net_downward_total_heat_flux",
               "wind stress module": "wind_stress_module",
               "mixing layer depth (Turbocline)": "turbocline",
               "Mixed Layer Depth 0.01 ref.10m": "turbocline_0.01"}

    checker = CFChecker()
    checker.xml_to_dict(xml_file)
    ds = xr.open_zarr("https://noc-msm-o.s3-ext.jc.rl.ac.uk/msm-repository/zarr-data/n06-2015.zarr")
    ds = checker.repair_metadata(ds, mapping)

