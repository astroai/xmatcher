# Pilot oracle: STILTS / lsdb

On CANFAR with real pinned trees:

1. Cone-extract the same HEALPix core (+margin) for spine and survey.
2. Run `stilts tmatch2` with the candidate pair radius from `pair_policy.toml`.
3. Run lsdb margin-cache crossmatch on the same inputs.
4. Compare pair sets to pensus `engine=fast` mutual-best edges:
   - recall / precision of edge ids
   - separation histogram KS distance
5. Record thresholds in the pilot report; fail the campaign if below gate.

Production tiles never call STILTS; oracle only.
