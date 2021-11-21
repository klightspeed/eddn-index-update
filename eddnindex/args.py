
from typing import Protocol


class ProcessorArgs(Protocol):
    reprocess: bool
    """
    --reprocess
        Reprocess files with unprocessed entries
    """

    reprocess_all: bool
    """
    --reprocess-all
        Reprocess all files
    """

    no_journal: bool
    """
    --no-journal
        Skip EDDN Journal messages
    """

    market: bool
    """
    --market
        Process market/shipyard/outfitting messages
    """

    nav_route: bool
    """
    --nav-route
        Process EDDN NavRoute messages
    """

    edsm_systems: bool
    """
    --edsm-systems
        Process EDSM systems dump
    """

    edsm_bodies: bool
    """
    --edsm-bodies
        Process EDSM bodies dump
    """

    edsm_missing_bodies: bool
    """
    --edsm-missing-bodies
        Process EDSM missing bodies
    """

    edsm_stations: bool
    """
    --edsm-stations
        Process EDSM stations dump
    """

    eddb_systems: bool
    """
    --eddb-systems
        Process EDDB systems dump
    """

    eddb_stations: bool
    """
    --eddb-stations
        Process EDDB stations dump
    """

    no_eddn: bool
    """
    --no-eddn
        Skip EDDN processing
    """

    process_title_progress: bool
    """
    --process-title-progress
        Update process title with progress
    """

    print_config: bool
    """
    --print-config
        Print config and exit
    """

    config_file: str
    """
    --config-file
        Configuration file
    """
