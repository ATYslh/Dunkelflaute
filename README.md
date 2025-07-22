Plan for calculations

3. check if capacity factors have already been calced
4. create_names for output files (something that contains the gcm,rcm, scenario and all the other jazz)
5. cut field into correct shape (depending on if EUR-11 or CEU-3)
6. Create wind
7. Apply powercurve to wind
8. Figure out when below 20% threshold
9. Apply PV to rsds
10. Apply 20% threshold
11. use logical_and to figure out when both are below threshold.
12. If file size is small enough: save files
13. Apply timmean_mean to create final output

Create wind histograms:
1. Create histogram for one examplatory hist data
2. fit curve to it for easier visibility
3. use the same bins for the same data again