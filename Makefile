ifneq (,$(wildcard ./.env))
    include .env
    export
endif

.PHONY: init build test report clean

init:
	@dotnet tool restore

build:
	@dotnet build

test:
	@dotnet test \
		--collect:"XPlat Code Coverage" \
		--logger trx \
		--results-directory:out/TestResults

report:
	@dotnet reportgenerator \
		-reports:"./out/TestResults/*/coverage.cobertura.xml" \
		-targetdir:"out/CoverageReport" \
		-reporttypes:"Html;Badges"

clean:
	@dotnet clean
	@-dotnet pwsh -NoLogo -NonInteractive -NoProfile -Command \
		Remove-Item -Recurse -Force out
