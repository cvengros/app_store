# hierarchy
Unpacks the user hierarchy from format (employee, immediate boss) to all pairs (employee, boss). Everyone is their own boss.

## Example
Input: A -> B (meaning A reports to B), B -> C 
This is unpacked to: A -> B, A -> C, A -> A, B -> C, B -> B, C -> C