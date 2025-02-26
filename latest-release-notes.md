### Release Notes
This is a breaking release with method renames and a signature change.

- **Added** - GitHub tags and releases, in addition to the NuGet publish.
- **Renamed** - `FliterBlock` to `FilterBlock` (typo).
- **Renamed** - Block Builder Methods to match the blocks names 1-for-1
	- `Select` -> `Transform`
	- `SelectMany` -> `TransformMany`
	- `Where` -> `Filter`
	- `ForEachAndComplete` -> `Action`
- **Signature Changed** - for the `ForEachAndComplete`/`Action` method. 
	- It now no longer auto-builds so that the builder pattern isn't different than the other blocks. This means you CAN build after an Action, although there shouldn't be a need for that.
	- **Just rename the method and add .Build() and everything will work the same**.