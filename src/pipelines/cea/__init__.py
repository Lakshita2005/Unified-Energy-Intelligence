"""CEA (Central Electricity Authority) ingestion pipelines.

* Daily Coal Stock Report (NPP .xls) -> ``pi.cea_coal_stock_daily``
* Daily Generation Report sub-report 10 (unit outages, NPP .xls) -> ``pi.cea_dgr_unit_outage``
* Daily Generation Report sub-report 2 (station generation, NPP .xls) -> ``pi.cea_dgr_plant_daily``
* Monthly Power Supply Position, energy and peak (cea.nic.in PDF) -> ``pi.cea_power_supply_position``

See ``docs/cea_pipeline.md`` and ``docs/cea_source_assessment.md``.
"""
