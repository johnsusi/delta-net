ifneq (,$(wildcard ./.env))
    include .env
    export
endif

.PHONY: init build test report clean

init:
	@dotnet tool restore

build:
	@dotnet build --configuration Release

test: build
	@dotnet test \
		--configuration Release \
		--no-build \
		--collect:"XPlat Code Coverage" \
		--logger trx \
		--results-directory:out/TestResults		

report: test
	@dotnet reportgenerator \
		-reports:"./out/TestResults/*/coverage.cobertura.xml" \
		-targetdir:"out/CoverageReport" \
		-reporttypes:"Html;Badges"

package: test
	@dotnet pack \
		--configuration Release \
		--no-build \
		--output out/

clean:
	@dotnet clean
	@-dotnet pwsh -NoLogo -NonInteractive -NoProfile -Command \
		Remove-Item -Recurse -Force out
